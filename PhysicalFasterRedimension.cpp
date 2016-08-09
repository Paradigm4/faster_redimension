/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2008-2016 SciDB, Inc.
* All Rights Reserved.
*
* faster_redimension is a plugin for SciDB, an Open Source Array DBMS maintained
* by Paradigm4. See http://www.paradigm4.com/
*
* faster_redimension is free software: you can redistribute it and/or modify
* it under the terms of the AFFERO GNU General Public License as published by
* the Free Software Foundation.
*
* faster_redimension is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
* INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
* NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
* the AFFERO GNU General Public License for the complete license terms.
*
* You should have received a copy of the AFFERO GNU General Public License
* along with faster_redimension.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
*
* END_COPYRIGHT
*/

#include <query/Operator.h>
#include <array/SortArray.h>
#include "FasterRedimensionSettings.h"
#include "ArrayIO.h"

namespace scidb
{

namespace faster_redimension
{

template <ArrayReadMode MODE>
class TupleDelegateArray : public SinglePassArray
{
private:
    typedef SinglePassArray super;
    size_t _rowIndex;
    Address _chunkAddressAtt0;
    Address _chunkAddressAtt1;
    Coordinates _posBuf;
    MemChunk _dataChunk;
    MemChunk _ebmChunk;
    std::weak_ptr<Query> _query;
    size_t const _chunkSize;
    ArrayReader<MODE> _reader;
    size_t const _chunkSizeLimit;
    Value _boolTrue;

public:
    TupleDelegateArray(shared_ptr<Array> & input, Settings const& settings, shared_ptr<Query>& query):
        super(settings.makeTupledSchema(query)),
        _rowIndex(0),
        _chunkAddressAtt0(0, Coordinates(3,0)),
        _chunkAddressAtt1(1, Coordinates(3,0)),
        _posBuf(3,0),
        _query(query),
        _chunkSize(settings.getTupledChunkSize()),
        _reader(input, settings),
        _chunkSizeLimit(MODE == READ_INPUT ? settings.getSortChunkSizeLimit() : settings.getSgChunkSizeLimit())
    {
        super::setEnforceHorizontalIteration(true);
        _chunkAddressAtt0.coords[0] -= _chunkSize;
        _chunkAddressAtt0.coords[2] = query->getInstanceID();
        _chunkAddressAtt1.coords[0] -= _chunkSize;
        _chunkAddressAtt1.coords[2] = query->getInstanceID();
        _boolTrue.setBool(true);
        if(!_reader.end() && MODE==READ_TUPLED)
        {
            _chunkAddressAtt0.coords[1] = RedimTuple::getInstanceId(_reader.getTuple());
            _chunkAddressAtt1.coords[1] = RedimTuple::getInstanceId(_reader.getTuple());
        }
    }

    size_t getCurrentRowIndex() const
    {
        return _rowIndex;
    }

    bool moveNext(size_t rowIndex)
    {
        if(_reader.end())
        {
            return false;
        }
        _chunkAddressAtt0.coords[0]+= _chunkSize;
        _chunkAddressAtt1.coords[0]+= _chunkSize;
        _dataChunk.initialize(this, &super::getArrayDesc(), _chunkAddressAtt0, 0);
        _posBuf = _chunkAddressAtt0.coords;
        Coordinate const limit = _posBuf[0] + _chunkSize;
        size_t numCells =0;
        shared_ptr<ChunkIterator> citer;
        citer = _dataChunk.getIterator(_query.lock(), ChunkIterator::SEQUENTIAL_WRITE | ChunkIterator::NO_EMPTY_CHECK);
        size_t totalSize = 0;
        while(!_reader.end() && _posBuf[0]<limit && totalSize < _chunkSizeLimit &&
               (MODE==READ_INPUT || RedimTuple::getInstanceId(_reader.getTuple()) == _chunkAddressAtt0.coords[1]))
        {
            Value const* tuple = _reader.getTuple();
            totalSize += tuple->size() + sizeof(Value);
            citer->setPosition(_posBuf);
            citer->writeItem(*_reader.getTuple());
            _reader.next();
            ++numCells;
            ++(_posBuf[0]);
        }
        citer->flush();
        citer.reset();
        _ebmChunk.initialize(this, &super::getArrayDesc(), _chunkAddressAtt1, 0);
        citer = _ebmChunk.getIterator(_query.lock(), ChunkIterator::SEQUENTIAL_WRITE | ChunkIterator::NO_EMPTY_CHECK);
        _posBuf = _chunkAddressAtt1.coords;
        while(numCells>0)
        {
            citer->setPosition(_posBuf);
            citer->writeItem(_boolTrue);
            --numCells;
            ++(_posBuf[0]);
        }
        ++_rowIndex;
        citer->flush();
        _dataChunk.setBitmapChunk(&_ebmChunk);
        if(!_reader.end() && MODE==READ_TUPLED && RedimTuple::getInstanceId(_reader.getTuple()) != _chunkAddressAtt0.coords[1])
        {
            _chunkAddressAtt0.coords[0] = 0 - _chunkSize;
            _chunkAddressAtt0.coords[1] = RedimTuple::getInstanceId(_reader.getTuple());
            _chunkAddressAtt1.coords[0] = 0 - _chunkSize;
            _chunkAddressAtt1.coords[1] = RedimTuple::getInstanceId(_reader.getTuple());
        }
        return true;
    }

    ConstChunk const& getChunk(AttributeID attr, size_t rowIndex)
    {
        if(attr==0)
        {
            return _dataChunk;
        }
        else if(attr == 1)
        {
            return _ebmChunk;
        }
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "internal inconsistency";
    }
};


class InputScannerChunkIterator : public ConstChunkIterator
{
private:
    ArrayReader<READ_INPUT>& _reader;
    size_t const _cellsPerChunkLimit;
    size_t const _binaryChunkSizeLimit;
    size_t _cellsRead;
    size_t _bytesRead;
    Coordinates _pos;

public:
    InputScannerChunkIterator(ArrayReader<READ_INPUT>& reader, size_t cellsPerChunkLimit, size_t binaryChunkSizeLimit):
        _reader(reader),
        _cellsPerChunkLimit(cellsPerChunkLimit),
        _binaryChunkSizeLimit(binaryChunkSizeLimit),
        _cellsRead(0),
        _bytesRead(0),
        _pos(1,0)
    {}

    virtual bool isEmpty() const
    {
        return false;
    }

    virtual Value const& getItem()
    {
        return *(_reader.getTuple());
    }

    virtual void operator ++()
    {
        ++_cellsRead;
        _bytesRead += (_reader.getTuple()->size() + sizeof(Value));
        _reader.next();
    }

    virtual bool end()
    {
        return _reader.end() || _cellsRead >= _cellsPerChunkLimit || _bytesRead >= _binaryChunkSizeLimit;
    }

    virtual Coordinates const& getPosition()
    {
        return _pos;
    }

    virtual int getMode() const                      { throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "illegal array chunkIterator getMode call"; }
    virtual bool setPosition(Coordinates const& pos) { throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "illegal array chunkIterator setPosition call"; }
    virtual void reset()                             { throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "illegal array chunkIterator reset call"; }
    ConstChunk const& getChunk()                     { throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "illegal array chunkIterator getChunk call"; }
    virtual std::shared_ptr<Query> getQuery()        { throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "illegal array chunkIterator getQuery call"; }
};

class InputScannerChunk : public ConstChunk
{
private:
    ArrayReader<READ_INPUT>& _reader;
    size_t const _cellsPerChunkLimit;
    size_t const _binaryChunkSizeLimit;

public:
    InputScannerChunk(ArrayReader<READ_INPUT>&  reader, size_t cellsPerChunkLimit, size_t binaryChunkSizeLimit):
        _reader(reader),
        _cellsPerChunkLimit(cellsPerChunkLimit),
        _binaryChunkSizeLimit(binaryChunkSizeLimit)
    {}

    virtual shared_ptr<ConstChunkIterator> getConstIterator(int iterationMode) const
    {
        return shared_ptr<ConstChunkIterator>(new InputScannerChunkIterator(_reader, _cellsPerChunkLimit, _binaryChunkSizeLimit));
    }

    virtual const ArrayDesc& getArrayDesc() const                              { throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "illegal array chunk getArrayDesc call"; }
    virtual const AttributeDesc& getAttributeDesc() const                      { throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "illegal array chunk getAttributeDesc call"; }
    virtual Coordinates const& getFirstPosition(bool withOverlap) const        { throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "illegal array chunk getFirstPosition call"; }
    virtual Coordinates const& getLastPosition(bool withOverlap) const         { throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "illegal array chunk getLastPosition call"; }
    virtual int getCompressionMethod() const                                   { throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "illegal array chunk getCompressionMethod call"; }
    virtual Array const& getArray() const                                      { throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "illegal array chunk getArray call"; }
};


class InputScannerArrayIterator : public ConstArrayIterator
{
private:
    ArrayReader<READ_INPUT>& _reader;
    InputScannerChunk _chunk;
    Coordinates _pos;
    size_t const _cellsPerChunkLimit;
    size_t const _binaryChunkSizeLimit;

public:
    InputScannerArrayIterator(ArrayReader<READ_INPUT>& reader, size_t cellsPerChunkLimit, size_t binaryChunkSizeLimit):
        _reader(reader),
        _chunk(_reader, cellsPerChunkLimit, binaryChunkSizeLimit),
        _pos(1,0),
        _cellsPerChunkLimit(cellsPerChunkLimit),
        _binaryChunkSizeLimit(binaryChunkSizeLimit)
    {}

    virtual ConstChunk const& getChunk()
    {
        return _chunk;
    }

    virtual bool end()
    {
        return _reader.end();
    }

    virtual void operator ++()
    {}

    virtual Coordinates const& getPosition()
    {
        return _pos;
    }

    virtual bool setPosition(Coordinates const& pos) { throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "illegal array iterator setPosition call"; }
    virtual void reset()                             { throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "illegal array iterator reset call"; }
};

class InputScannerArray : public Array
{
private:
    ArrayDesc _desc;
    mutable ArrayReader<READ_INPUT> _reader;
    size_t const _cellsPerChunkLimit;
    size_t const _binaryChunkSizeLimit;

public:
    InputScannerArray(shared_ptr<Array>& inputArray, Settings const& settings,  shared_ptr<Query>& query):
        _desc(settings.makePreSortSchema(query)),
        _reader(inputArray, settings),
        _cellsPerChunkLimit(settings.getTupledChunkSize()),
        _binaryChunkSizeLimit(settings.getSortChunkSizeLimit())
    {}

    virtual ArrayDesc const& getArrayDesc() const
    {
        return _desc;
    }

    virtual Access getSupportedAccess() const
    {
        return SINGLE_PASS; //Don't multithread on me! (tm)
    }

    virtual std::shared_ptr<ConstArrayIterator> getConstIterator(AttributeID attr) const
    {
        return shared_ptr<ConstArrayIterator>(new InputScannerArrayIterator(_reader, _cellsPerChunkLimit, _binaryChunkSizeLimit));
    }
};

}

using namespace std;
using namespace faster_redimension;

class PhysicalFasterRedimension : public PhysicalOperator
{
public:
    PhysicalFasterRedimension(string const& logicalName,
                             string const& physicalName,
                             Parameters const& parameters,
                             ArrayDesc const& schema):
         PhysicalOperator(logicalName, physicalName, parameters, schema)
    {}

    virtual bool changesDistribution(std::vector<ArrayDesc> const&) const
    {
        return true;
    }

    virtual RedistributeContext getOutputDistribution(
               std::vector<RedistributeContext> const& inputDistributions,
               std::vector< ArrayDesc> const& inputSchemas) const
    {
        return RedistributeContext(createDistribution(psUndefined), _schema.getResidency() );
    }

    template<ArrayReadMode READ_MODE, ArrayWriteMode WRITE_MODE>
    shared_ptr<Array> arrayPass(shared_ptr<Array>& data, shared_ptr<Query>& query, Settings const& settings)
    {
        ArrayReader<READ_MODE> reader(data, settings);
        ArrayWriter<WRITE_MODE> writer(settings, query);
        while(!reader.end())
        {
            writer.writeTuple(reader.getTuple());
            reader.next();
        }
        return writer.finalize();
    }

    shared_ptr<Array> sortArray(shared_ptr<Array> & tupledArray, shared_ptr<Query>& query, Settings const& settings)
    {
        arena::Options options;
        options.name  ("FR sort");
        options.parent(_arena);
        options.threading(false);
        arena::ArenaPtr sortArena = arena::newArena(options);
        SortingAttributeInfos sortingAttributeInfos(1);
        sortingAttributeInfos[0].columnNo = 0;
        sortingAttributeInfos[0].ascent = true;
        SortArray sorter(settings.makeTupledSchema(query, true), sortArena, false, settings.getTupledChunkSize());
        shared_ptr<TupleComparator> tcomp(make_shared<TupleComparator>(sortingAttributeInfos, tupledArray->getArrayDesc()));
        return sorter.getSortedArray(tupledArray, query, tcomp);
    }

    shared_ptr<Array> globalMerge(shared_ptr<Array>& tupled, shared_ptr<Query>& query, Settings const& settings)
    {
        ArrayWriter<WRITE_OUTPUT> output(settings, query);
        size_t const numInstances = query->getInstancesCount();
        vector<shared_ptr<ConstArrayIterator> > aiters(numInstances);
        vector<shared_ptr<ConstChunkIterator> > citers(numInstances);
        vector<Coordinates > positions(numInstances);
        size_t numClosed = 0;
        for(size_t inst =0; inst<numInstances; ++inst)
        {
            positions[inst].resize(3);
            positions[inst][0] = 0;
            positions[inst][1] = query->getInstanceID();
            positions[inst][2] = inst;
            aiters[inst] = tupled->getConstIterator(0);
            if(!aiters[inst]->setPosition(positions[inst]))
            {
                aiters[inst].reset();
                citers[inst].reset();
                numClosed++;
            }
            else
            {
                citers[inst] = aiters[inst]->getChunk().getConstIterator();
            }
        }
        while(numClosed < numInstances)
        {
            Value const* minTuple = NULL;
            size_t toAdvance;
            for(size_t inst=0; inst<numInstances; ++inst)
            {
                if(citers[inst] == 0)
                {
                    continue;
                }
                Value const* tuple = &(citers[inst]->getItem());
                if(minTuple == NULL || RedimTuple::redimTupleLess(tuple, minTuple))
                {
                    minTuple = tuple;
                    toAdvance=inst;
                }
            }
            output.writeTuple(minTuple);
            ++(*citers[toAdvance]);
            if(citers[toAdvance]->end())
            {
                positions[toAdvance][0] = positions[toAdvance][0] + settings.getTupledChunkSize();
                if(!aiters[toAdvance]->setPosition(positions[toAdvance]))
                {
                    aiters[toAdvance].reset();
                    citers[toAdvance].reset();
                    numClosed++;
                }
                else
                {
                    citers[toAdvance] = aiters[toAdvance]->getChunk().getConstIterator();
                }
            }
        }
        return output.finalize();
    }

    shared_ptr< Array> execute(vector< shared_ptr< Array> >& inputArrays, shared_ptr<Query> query)
    {
        ArrayDesc const& inputSchema = inputArrays[0]->getArrayDesc();
        Settings settings(inputSchema, _schema, query);
        shared_ptr<Array>& inputArray = inputArrays[0];

        //inputArray = shared_ptr<Array>(new TupleDelegateArray<READ_INPUT>(inputArray,settings, query));
        inputArray = shared_ptr<Array>(new InputScannerArray(inputArray,settings, query));
        inputArray = sortArray(inputArray, query, settings);



        inputArray = shared_ptr<Array>(new TupleDelegateArray<READ_TUPLED>(inputArray,settings, query));
        inputArray = redistributeToRandomAccess(inputArray, createDistribution(psByCol),query->getDefaultArrayResidency(), query, false);
        return globalMerge(inputArray, query, settings);
    }
};

REGISTER_PHYSICAL_OPERATOR_FACTORY(PhysicalFasterRedimension, "faster_redimension", "physical_faster_redimension");
} //namespace scidb
