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
#include <array/RLE.h>
#include "FasterRedimensionSettings.h"
#include "ArrayIO.h"

namespace scidb
{

namespace faster_redimension
{

///////////////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////////

/*
 * This is a delegate-style array for reading the input into the sort. It wraps around ArrayReader<READ_INPUT>
 * and only implements the methods that the sort needs. It also makes sure no chunk passed to sort exceeds
 * a binary size limit. Written out upside-down.
 * InputScannerChunkIterator
 * InputScannerChunk
 * InputScannerArrayIterator
 * InputScannerArray
 */
class InputScannerChunkIterator : public ConstChunkIterator
{
private:
    ArrayReader<READ_INPUT>& _reader;
    size_t const _binaryChunkSizeLimit;
    size_t _cellsRead;
    size_t _bytesRead;
    Coordinates _pos;

public:
    InputScannerChunkIterator(ArrayReader<READ_INPUT>& reader, size_t binaryChunkSizeLimit):
        _reader(reader),
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
        return _reader.end() || _bytesRead >= _binaryChunkSizeLimit;
    }

    virtual Coordinates const& getPosition()
    {
        //XXX:Sorter calls this but doesn't use it for anything
        return _pos;
    }

    virtual int getMode() const                      { throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "illegal array chunkIterator getMode call"; }
    virtual bool setPosition(Coordinates const& pos) { throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "illegal array chunkIterator setPosition call"; }
    virtual void restart()                             { throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "illegal array chunkIterator restart call"; }
    ConstChunk const& getChunk()                     { throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "illegal array chunkIterator getChunk call"; }
    virtual std::shared_ptr<Query> getQuery()        { throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "illegal array chunkIterator getQuery call"; }
};

class InputScannerChunk : public ConstChunk
{
private:
    ArrayReader<READ_INPUT>& _reader;
    size_t const _binaryChunkSizeLimit;

public:
    InputScannerChunk(ArrayReader<READ_INPUT>&  reader, size_t binaryChunkSizeLimit):
        _reader(reader),
        _binaryChunkSizeLimit(binaryChunkSizeLimit)
    {}

    virtual shared_ptr<ConstChunkIterator> getConstIterator(int iterationMode) const
    {
        return shared_ptr<ConstChunkIterator>(new InputScannerChunkIterator(_reader, _binaryChunkSizeLimit));
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
    size_t const _binaryChunkSizeLimit;

public:
    InputScannerArrayIterator(ArrayReader<READ_INPUT>& reader, size_t binaryChunkSizeLimit):
        _reader(reader),
        _chunk(_reader, binaryChunkSizeLimit),
        _pos(1,0),
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
        //XXX:Sorter calls this but doesn't use it for anything
        return _pos;
    }

    virtual bool setPosition(Coordinates const& pos) { throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "illegal array iterator setPosition call"; }
    virtual void restart()                             { throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "illegal array iterator restart call"; }
};

class InputScannerArray : public Array
{
private:
    ArrayDesc _desc;
    mutable ArrayReader<READ_INPUT> _reader;
    size_t const _binaryChunkSizeLimit;

public:
    InputScannerArray(shared_ptr<Array>& inputArray, Settings const& settings,  shared_ptr<Query>& query):
        _desc(settings.makePreSortSchema(query)),
        _reader(inputArray, settings),
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
        return shared_ptr<ConstArrayIterator>(new InputScannerArrayIterator(_reader, _binaryChunkSizeLimit));
    }
};

///////////////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////////
/*
 * Now, to avoid excessive chunkIterator::writeItem calls we pack the RedimensionTuples into large binary blobs:
 * [uint32 size][tuple][uint32 size][tuple]...[uint32 size][tuple][uint32 0]
 * Note the use of 0 as the terminator.
 */

//Distance between chunk start and the data region (for a chunk with a single binary blob)
static size_t getChunkOverheadSize()
{
    return             (  sizeof(ConstRLEPayload::Header) +
                                 2 * sizeof(ConstRLEPayload::Segment) +
                                 sizeof(varpart_offset_t) + 5);
}

//Distance between chunk start and the size pointer (for a chunk with a single binary blob)
static size_t getSizeOffset()
{
    return getChunkOverheadSize()-4;
}

/*
 * Wrap around ArrayReader<READ_TUPLED> and output the sg schema chunks with tuples packed into blobs - ready for SG.
 */
class TupleSgArray : public SinglePassArray
{
private:
    typedef SinglePassArray super;
    size_t _rowIndex;
    Address _chunkAddress;
    Coordinates _posBuf;
    MemChunk _chunk;
    std::weak_ptr<Query> _query;
    size_t const _binaryChunkSizeLimit;
    size_t const _chunkOverheadSize;
    ArrayReader<READ_TUPLED> _reader;
    char* _bufPointer;
    uint32_t* _sizePointer;

public:
    TupleSgArray(shared_ptr<Array> & input, Settings const& settings, shared_ptr<Query>& query):
        super(settings.makeSgSchema(query)),
        _rowIndex(0),
        _chunkAddress(0, Coordinates(3,0)),
        _posBuf(3,0),
        _query(query),
        _binaryChunkSizeLimit(settings.getSgChunkSizeLimit()),
        _chunkOverheadSize(getChunkOverheadSize()),
        _reader(input, settings)
    {
        super::setEnforceHorizontalIteration(true);
        _chunkAddress.coords[0]=-1;
        _chunkAddress.coords[2] = query->getInstanceID();
        if(!_reader.end())
        {
            _chunkAddress.coords[1] = RedimTuple::getInstanceId(_reader.getTuple());
        }
        try
        {
            _chunk.allocate(_chunkOverheadSize + _binaryChunkSizeLimit);
        }
        catch(...)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "TupleSgArray cannot allocate memory";
        }
        _bufPointer = (char*) _chunk.getData();
        ConstRLEPayload::Header* hdr = (ConstRLEPayload::Header*) _bufPointer;
        hdr->_magic = RLE_PAYLOAD_MAGIC;
        hdr->_nSegs = 1;
        hdr->_elemSize = 0;
        hdr->_dataSize = _binaryChunkSizeLimit + 5 + sizeof(varpart_offset_t);
        hdr->_varOffs = sizeof(varpart_offset_t);
        hdr->_isBoolean = 0;
        ConstRLEPayload::Segment* seg = (ConstRLEPayload::Segment*) (hdr+1);
        *seg =  ConstRLEPayload::Segment(0,0,false,false);
        ++seg;
        *seg =  ConstRLEPayload::Segment(1,0,false,false);
        varpart_offset_t* vp =  reinterpret_cast<varpart_offset_t*>(seg+1);
        *vp = 0;
        uint8_t* sizeFlag = reinterpret_cast<uint8_t*>(vp+1);
        *sizeFlag =0;
        _sizePointer = reinterpret_cast<uint32_t*> (sizeFlag + 1);
        *_sizePointer = static_cast<uint32_t>(_binaryChunkSizeLimit);
        _bufPointer = reinterpret_cast<char*> (_sizePointer+1);
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
        _bufPointer = reinterpret_cast<char*> (_sizePointer+1);
        _chunkAddress.coords[0]++;
        _chunk.initialize(this, &super::getArrayDesc(), _chunkAddress, 0);
        size_t dataSize = 0;
        while(!_reader.end() && (dataSize + _reader.getTuple()->size() + 2*sizeof(uint32_t)) < _binaryChunkSizeLimit &&
                RedimTuple::getInstanceId(_reader.getTuple()) == _chunkAddress.coords[1])
        {
            Value const* tuple = _reader.getTuple();
            uint32_t const tupleSize = tuple->size();
            uint32_t* sizePtr = reinterpret_cast<uint32_t*>(_bufPointer);
            dataSize += (tupleSize + sizeof(uint32_t));
            *sizePtr = tupleSize;
            ++sizePtr;
            _bufPointer = reinterpret_cast<char*>(sizePtr);
            memcpy(_bufPointer, tuple->data(), tupleSize);
            _bufPointer += tupleSize;
            _reader.next();
        }
        if(dataSize == 0)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "tuples too large for chunks; raise the memory limit";
        }
        else
        {
            uint32_t* terminatorPtr = reinterpret_cast<uint32_t*>(_bufPointer);
            *terminatorPtr = 0;
            dataSize += sizeof(uint32_t);
            *_sizePointer = static_cast<uint32_t>(dataSize);
        }
        ++_rowIndex;
        if(!_reader.end() && RedimTuple::getInstanceId(_reader.getTuple()) != _chunkAddress.coords[1])
        {
            _chunkAddress.coords[0] = -1;
            _chunkAddress.coords[1] = RedimTuple::getInstanceId(_reader.getTuple());
        }
        return true;
    }

    ConstChunk const& getChunk(AttributeID attr, size_t rowIndex)
    {
        if(attr==0)
        {
            return _chunk;
        }
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "internal inconsistency";
    }
};

/*
 * Wrap around a single chunk out of the SG and pull tuples out of it.
 */
class ChunkTupleUnpacker
{
private:
    size_t const _overheadSize;
    size_t const _sizeOffset;
    ConstChunk const* _chunkPtr;
    char *_readPtr;
    Value _tupleBuf;

public:
    ChunkTupleUnpacker():
        _overheadSize(getChunkOverheadSize()),
        _sizeOffset(getSizeOffset()),
        _chunkPtr(0),
        _readPtr(0)
    {}

    ~ChunkTupleUnpacker()
    {
        if(_chunkPtr)
        {
            _chunkPtr->unPin();
            _chunkPtr = NULL;
        }
    }

    void setChunk(ConstChunk const* chunk)
    {
        if(_chunkPtr)
        {
            _chunkPtr->unPin();
        }
        _chunkPtr = chunk;
        _chunkPtr->pin();
        uint32_t chunkSize = *(reinterpret_cast<uint32_t*>(reinterpret_cast<char*>(_chunkPtr->getData()) + _sizeOffset));
        if(chunkSize == 0)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "[defensive] encountered a chunk with no data.";
        }
        _readPtr = reinterpret_cast<char*>(_chunkPtr->getData()) + _overheadSize;
        uint32_t* tupleSizePtr = reinterpret_cast<uint32_t*>(_readPtr);
        uint32_t const tupleSize = *tupleSizePtr;
        if(tupleSize == 0)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "[defensive] encountered a chunk with the first zero tuple.";
        }
        ++tupleSizePtr;
        _readPtr = reinterpret_cast<char*> (tupleSizePtr);
        _tupleBuf.setData(_readPtr, tupleSize);
        _readPtr += tupleSize;
    }

    Value const* getTuple()
    {
        return &_tupleBuf;
    }

    void next()
    {
        if(_chunkPtr == NULL)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "internal inconsistency";
        }
        uint32_t* tupleSizePtr = reinterpret_cast<uint32_t*>(_readPtr);
        uint32_t const tupleSize = *tupleSizePtr;
        if(tupleSize == 0)
        {
            _chunkPtr->unPin();
            _chunkPtr = NULL;
            return;
        }
        ++tupleSizePtr;
        _readPtr = reinterpret_cast<char*> (tupleSizePtr);
        _tupleBuf.setData(_readPtr, tupleSize);
        _readPtr += tupleSize;
    }

    bool end()
    {
        return (_chunkPtr == NULL);
    }

    void clear()
    {
        if(_chunkPtr != NULL)
        {
            _chunkPtr->unPin();
            _chunkPtr = NULL;
        }
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
        SortArray sorter(settings.makePreSortSchema(query, true), sortArena, false, settings.getSortedArrayChunkSize());
        shared_ptr<TupleComparator> tcomp(make_shared<TupleComparator>(sortingAttributeInfos, tupledArray->getArrayDesc()));
        return sorter.getSortedArray(tupledArray, query, tcomp);
    }

    shared_ptr<Array> globalMerge(shared_ptr<Array>& tupled, shared_ptr<Query>& query, Settings const& settings)
    {
        OutputWriter output(settings, query);
        size_t const numInstances = query->getInstancesCount();
        vector<shared_ptr<ConstArrayIterator> > aiters(numInstances);
        vector<ChunkTupleUnpacker> unpackers(numInstances);
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
                unpackers[inst].clear();
                numClosed++;
            }
            else
            {
                unpackers[inst].setChunk(&aiters[inst]->getChunk());
            }
        }
        while(numClosed < numInstances)
        {
            Value const* minTuple = NULL;
            size_t toAdvance;
            for(size_t inst=0; inst<numInstances; ++inst)
            {
                if(unpackers[inst].end())
                {
                    continue;
                }
                Value const* tuple = unpackers[inst].getTuple();
                if(minTuple == NULL || RedimTuple::redimTupleLess(tuple, minTuple))
                {
                    minTuple = tuple;
                    toAdvance=inst;
                }
            }
            output.writeTuple(minTuple);
            unpackers[toAdvance].next();
            if(unpackers[toAdvance].end())
            {
                positions[toAdvance][0] = positions[toAdvance][0] + 1;
                if(!aiters[toAdvance]->setPosition(positions[toAdvance]))
                {
                    aiters[toAdvance].reset();
                    unpackers[toAdvance].clear();
                    numClosed++;
                }
                else
                {
                    unpackers[toAdvance].setChunk(&aiters[toAdvance]->getChunk());
                }
            }
        }
        return output.finalize();
    }

    shared_ptr< Array> execute(vector< shared_ptr< Array> >& inputArrays, shared_ptr<Query> query)
    {
        ArrayDesc const& inputSchema = inputArrays[0]->getArrayDesc();
        Settings settings(inputSchema, _schema, _parameters, false, query);
        shared_ptr<Array>& inputArray = inputArrays[0];
        inputArray = shared_ptr<Array>(new InputScannerArray(inputArray,settings, query));
        inputArray = sortArray(inputArray, query, settings);
        inputArray = shared_ptr<Array>(new TupleSgArray(inputArray,settings, query));
        inputArray = redistributeToRandomAccess(inputArray, createDistribution(psByCol),query->getDefaultArrayResidency(), query, false);
        return globalMerge(inputArray, query, settings);
    }
};

REGISTER_PHYSICAL_OPERATOR_FACTORY(PhysicalFasterRedimension, "faster_redimension", "physical_faster_redimension");
} //namespace scidb
