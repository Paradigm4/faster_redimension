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

#ifndef ARRAYIO_H_
#define ARRAYIO_H_

#include "FasterRedimensionSettings.h"
#include "TupleAddress.h"
#include <util/Network.h>

namespace scidb
{
namespace faster_redimension
{

enum ArrayReadMode
{
    READ_INPUT,          //we're reading the input array, however it may be; we reorder attributes and convert dimensions to tuple if needed;
                         //here we filter dimensions for nulls
    READ_TUPLED,         //we're reading an array that's been tupled
};

template<ArrayReadMode MODE>
class ArrayReader
{
private:
    shared_ptr<Array>                       _input;
    Settings const&                         _settings;
    vector<Value const*>                    _tuple;
    size_t const                            _nOutputDimensions;
    size_t const                            _nOutputAttributes;
    size_t const                            _nInputAttributes;
    vector<size_t> const&                   _inputAttributes;             //indeces of the attributes we're reading;
    vector<size_t> const&                   _inputAttributeDestinations;  //map each attribute we're reading to tuple
    vector<bool>   const&                   _inputAttributeFilterNull;    //true if the respective attribute should be filtered for NULL
    size_t const                            _nInputDimensions;
    vector<size_t> const&                   _inputDimensions;              //indece of the dimensions we're reading
    vector<size_t> const&                   _inputDimensionDestinations;   //map each dimension we're reading to tuple
    vector<Value>                           _dimVals;
    size_t const                            _numIterators;
    vector<shared_ptr<ConstArrayIterator> > _aiters;
    vector<shared_ptr<ConstChunkIterator> > _citers;
    Coordinates                             _cellPos;
    Coordinates                             _chunkPos;
    uint32_t                                _dstInstanceId;
    position_t                              _cellLPos;
    Value                                   _tupleAddress;


public:
    ArrayReader( shared_ptr<Array>& input, Settings const& settings):
        _input(input),
        _settings(settings),
        _tuple(settings.getTupleSize()),
        _nOutputDimensions(settings.getNumOutputDims()),
        _nOutputAttributes(settings.getNumOutputAttrs()),
        _nInputAttributes(MODE == READ_INPUT ? settings.getNumInputAttributesRead() : settings.getTupleSize()),
        _inputAttributes(settings.getInputAttributesRead()),
        _inputAttributeDestinations(settings.getInputAttributeDestinations()),
        _inputAttributeFilterNull(settings.getInputAttributeFilterNull()),
        _nInputDimensions(settings.getNumInputDimensionsRead()),
        _inputDimensions(settings.getInputDimensionsRead()),
        _inputDimensionDestinations(settings.getInputDimensionDestinations()),
        _dimVals(_nInputDimensions),
        _numIterators( _nInputAttributes ==0 ? 1 : _nInputAttributes ),
        _aiters(_numIterators),
        _citers(_numIterators),
        _cellPos(_nOutputDimensions),
        _chunkPos(_nOutputDimensions)
    {
        for(size_t i =0; i<_numIterators; ++i)
        {
            if(_nInputAttributes ==0)
            {
                _aiters[i] = _input->getConstIterator(_input->getArrayDesc().getAttributes(true).size()); //empty tag
            }
            else if( MODE==READ_INPUT)
            {
                _aiters[i] = _input->getConstIterator(_inputAttributes[i]);
            }
            else
            {
                _aiters[i] = input->getConstIterator(i);
            }
        }
        if(MODE == READ_INPUT)
        {
            _tuple[settings.getTupleSize()-1] = &_tupleAddress;
        }
        if(!end())
        {
            next<true>();
        }
    }

private:
    bool setAndCheckTuple()
    {
        for(size_t i =0; i<_nInputAttributes; ++i)
        {
            Value const* item = &(_citers[i]->getItem());
            if(MODE == READ_INPUT)
            {
                if(_inputAttributeFilterNull[i] && item->isNull())
                {
                    return false;
                }
                size_t idx = MODE==READ_INPUT ? _inputAttributeDestinations[i] : i;
                if(idx < _nOutputAttributes)
                {
                    _tuple[idx] = item;
                }
                else
                {
                    _cellPos[idx - _nOutputAttributes] = item->getInt64();
                }
            }
            else
            {
                _tuple[i] = item;
            }
        }
        if(MODE==READ_TUPLED)
        {
            return true;
        }
        Coordinates const& pos = _citers[0]->getPosition();
        for(size_t i =0; i<_nInputDimensions; ++i)
        {
            Coordinate c = pos[_inputDimensions[i]];
            size_t idx = _inputDimensionDestinations[i];
            if(idx < _nOutputAttributes)
            {
                _dimVals[i].setInt64(c);
                _tuple[idx] = &_dimVals[i];
            }
            else
            {
                _cellPos[idx - _nOutputAttributes] = c;
            }
        }
        _chunkPos = _cellPos;
        _settings.getOutputChunkPosition(_chunkPos);
        _dstInstanceId = _settings.getInstanceForChunk(_chunkPos);
        _cellLPos = _settings.getOutputCellPos(_chunkPos, _cellPos);
        makeTupleAddress(_dstInstanceId, _chunkPos, _cellLPos, _nOutputDimensions, &_tupleAddress);
        return true; //we got a valid tuple!
    }

    bool findNextTupleInChunk()
    {
        while(!_citers[0]->end())
        {
            if(setAndCheckTuple())
            {
                return true;
            }
            for(size_t i =0; i<_numIterators; ++i)
            {
                ++(*_citers[i]);
            }
        }
        return false;
    }

public:
    template <bool FIRST_ITERATION = false>
    void next()
    {
        if(end())
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Internal inconsistency";
        }
        if(!FIRST_ITERATION)
        {
            for(size_t i =0; i<_numIterators; ++i)
            {
                ++(*_citers[i]);
            }
            if(findNextTupleInChunk())
            {
                return;
            }
            for(size_t i =0; i<_numIterators; ++i)
            {
                ++(*_aiters[i]);
            }
        }
        while(!_aiters[0]->end())
        {
            for(size_t i =0; i<_numIterators; ++i)
            {
                _citers[i] = _aiters[i]->getChunk().getConstIterator();
            }
            if(findNextTupleInChunk())
            {
                return;
            }
            for(size_t i =0; i<_numIterators; ++i)
            {
                ++(*_aiters[i]);
            }
        }
    }

    bool end()
    {
        return _aiters[0]->end();
    }

    vector<Value const*> const& getTuple()
    {
        if(end())
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Internal inconsistency";
        }
        return _tuple;
    }
};


enum ArrayWriteMode
{
    WRITE_TUPLED,
    WRITE_SPLIT_ON_INSTANCE,
    WRITE_OUTPUT
};

template<ArrayWriteMode MODE>
class ArrayWriter : public boost::noncopyable
{
private:
    shared_ptr<Array>                   _output;
    InstanceID const                    _myInstanceId;
    size_t const                        _numInstances;
    size_t const                        _numAttributes;
    size_t const                        _numTupleDimensions;
    size_t const                        _chunkSize;
    shared_ptr<Query>                   _query;
    Settings const&                     _settings;
    Coordinates                         _outputChunkPosition;
    Coordinates                         _outputPosition;
    Coordinates                         _outputChunkPositionBuf;
    Coordinates                         _outputPositionBuf;
    vector<shared_ptr<ArrayIterator> >  _arrayIterators;
    vector<shared_ptr<ChunkIterator> >  _chunkIterators;
    size_t                              _currentDstInstanceId;
    Value                               _boolTrue;


public:
    ArrayWriter(Settings const& settings, shared_ptr<Query> const& query):
        _output               (std::make_shared<MemArray>( MODE==WRITE_OUTPUT ? settings.getOutputSchema() : settings.makeTupledSchema(query), query)),
        _myInstanceId         (query->getInstanceID()),
        _numInstances         (query->getInstancesCount()),
        _numAttributes        (_output->getArrayDesc().getAttributes(true).size() ),
        _numTupleDimensions   (settings.getNumOutputDims()),
        _chunkSize            (settings.getTupledChunkSize()),
        _query                (query),
        _settings             (settings),
        _outputChunkPosition     ( MODE == WRITE_OUTPUT ? 0 : 3, 0),
        _outputPosition          ( MODE == WRITE_OUTPUT ? 0 : 3, 0),
        _outputChunkPositionBuf  ( MODE == WRITE_OUTPUT ? _output->getArrayDesc().getDimensions().size() : 0, 0),
        _outputPositionBuf       ( MODE == WRITE_OUTPUT ? _output->getArrayDesc().getDimensions().size() : 0, 0),
        _arrayIterators       (_numAttributes+1, NULL),
        _chunkIterators       (_numAttributes+1, NULL),
        _currentDstInstanceId (0)
    {
        _boolTrue.setBool(true);
        for(size_t i =0; i<_numAttributes+1; ++i)
        {
            _arrayIterators[i] = _output->getIterator(i);
        }
        if (MODE != WRITE_OUTPUT)
        {
            _outputPosition[2] = _myInstanceId;
        }
    }

    void writeTuple(vector<Value const*> const& tuple)
    {
        bool newChunk = false;
        if(MODE == WRITE_SPLIT_ON_INSTANCE)
        {
            uint32_t dstInstanceId = getInstanceId(tuple[ _numAttributes-1 ]);
            if(dstInstanceId != _currentDstInstanceId)
            {
                _currentDstInstanceId = dstInstanceId;
                _outputPosition[0] = 0;
                _outputPosition[1] = _currentDstInstanceId;
                _outputChunkPosition = _outputPosition;
                newChunk = true;
            }
        }
        if (MODE != WRITE_OUTPUT && _outputPosition[0] % _chunkSize == 0)
        {
            _outputChunkPosition = _outputPosition;
            newChunk = true;
        }
        else if (MODE == WRITE_OUTPUT)
        {
            getChunkPos(tuple[_numAttributes], _numTupleDimensions, _outputChunkPositionBuf);
            position_t cellPos = getCellPos(tuple[_numAttributes], _numTupleDimensions);
            _settings.getOutputCellCoords(_outputChunkPositionBuf, cellPos, _outputPositionBuf);
            if(_outputChunkPosition.size() == 0) //first one!
            {
                _outputChunkPosition = _outputChunkPositionBuf;
                newChunk = true;
            }
            else if(_outputChunkPositionBuf != _outputChunkPosition)
            {
                _outputChunkPosition = _outputChunkPositionBuf;
                newChunk = true;
            }
            else if(_outputPositionBuf == _outputPosition)
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Data collision";
            }
            _outputPosition = _outputPositionBuf;
        }
        if( newChunk )
        {
            for(size_t i=0; i<_numAttributes+1; ++i)
            {
                if(_chunkIterators[i].get())
                {
                    _chunkIterators[i]->flush();
                }
                _chunkIterators[i] = _arrayIterators[i]->newChunk(_outputChunkPosition).getIterator(_query, ChunkIterator::SEQUENTIAL_WRITE | ChunkIterator::NO_EMPTY_CHECK );
            }
        }
        for(size_t i=0; i<_numAttributes; ++i)
        {
            _chunkIterators[i]->setPosition(_outputPosition);
            _chunkIterators[i]->writeItem(*(tuple[i]));
        }
        _chunkIterators[_numAttributes]->setPosition(_outputPosition);
        _chunkIterators[_numAttributes]->writeItem(_boolTrue);
        if(MODE != WRITE_OUTPUT)
        {
            ++_outputPosition[ 0 ];
        }
    }

    shared_ptr<Array> finalize()
    {
        for(size_t i =0; i<_numAttributes+1; ++i)
        {
            if(_chunkIterators[i].get())
            {
                _chunkIterators[i]->flush();
            }
            _chunkIterators[i].reset();
            _arrayIterators[i].reset();
        }
        shared_ptr<Array> result = _output;
        _output.reset();
        return result;
    }
};


} } //namespaces




#endif /* ARRAYIO_H_ */
