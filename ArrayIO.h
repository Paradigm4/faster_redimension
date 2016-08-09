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
#include <util/Network.h>
#include "RedimensionTuple.h"

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
    size_t const                            _numIterators;
    vector<Value const*>                    _tupleInputs;
    vector<Value>                           _inputDimensionVals;
    vector<shared_ptr<ConstArrayIterator> > _aiters;
    vector<shared_ptr<ConstChunkIterator> > _citers;
    Coordinates                             _cellCoords;
    Coordinates                             _chunkCoords;
    uint32_t                                _dstInstanceId;
    position_t                              _cellLPos;
    Value                                   _tupleValue;
    Value const*                            _tupleOutput;


public:
    ArrayReader( shared_ptr<Array>& input, Settings const& settings):
        _input(input),
        _settings(settings),
        _numIterators( MODE==READ_TUPLED ? 1 : (_settings.getNumInputAttributesRead() > 0 ? _settings.getNumInputAttributesRead() : 1)),
        _tupleInputs( MODE== READ_INPUT ? _settings.getNumOutputAttrs() : 0),
        _inputDimensionVals(MODE== READ_INPUT ? _settings.getNumInputDimensionsRead() : 0),
        _aiters(_numIterators),
        _citers(_numIterators),
        _cellCoords(_settings.getNumOutputDims()),
        _chunkCoords(_settings.getNumOutputDims())
    {
        for(size_t i =0; i<_numIterators; ++i)
        {
            if(_settings.getNumInputAttributesRead() ==0)
            {
                _aiters[i] = _input->getConstIterator(_input->getArrayDesc().getAttributes(true).size()); //empty tag
            }
            else if( MODE==READ_INPUT)
            {
                _aiters[i] = _input->getConstIterator(_settings.getInputAttributesRead()[i]);
            }
            else
            {
                _aiters[i] = input->getConstIterator(i);
            }
        }
        if(MODE == READ_INPUT)
        {
            _tupleOutput = &_tupleValue;
        }
        if(!end())
        {
            next<true>();
        }
    }

private:
    bool setAndCheckTuple()
    {
        if(MODE==READ_TUPLED)
        {
            _tupleOutput = &(_citers[0]->getItem());
            return true;
        }
        for(size_t i =0; i<_settings.getNumInputAttributesRead(); ++i)
        {
            Value const* item = &(_citers[i]->getItem());
            if(_settings.getInputAttributeFilterNull()[i] && item->isNull())
            {
                return false;
            }
            size_t idx = _settings.getInputAttributeDestinations()[i];
            if(idx < _settings.getNumOutputAttrs())
            {
                _tupleInputs[idx] = item;
            }
            else
            {
                _cellCoords[idx - _settings.getNumOutputAttrs()] = item->getInt64();
            }
        }
        Coordinates const& pos = _citers[0]->getPosition();
        for(size_t i =0; i<_settings.getNumInputDimensionsRead(); ++i)
        {
            Coordinate coord = pos[_settings.getInputDimensionsRead()[i]];
            size_t idx = _settings.getInputDimensionDestinations()[i];
            if(idx < _settings.getNumOutputAttrs())
            {
                _inputDimensionVals[i].setInt64(coord);
                _tupleInputs[idx] = &_inputDimensionVals[i];
            }
            else
            {
                _cellCoords[idx - _settings.getNumOutputAttrs()] = coord;
            }
        }
        _chunkCoords = _cellCoords;
        _settings.getOutputChunkPosition(_chunkCoords);
        _dstInstanceId = _settings.getInstanceForChunk(_chunkCoords);
        _cellLPos = _settings.getOutputCellPos(_chunkCoords, _cellCoords);
        RedimTuple::makeRedimTuple(_settings.getNumOutputDims(),
                                   _settings.getNumOutputAttrs(),
                                   _settings.outputAttributeNullable(),
                                   _settings.getOutputAttributeSizes(),
                                   _dstInstanceId,
                                   _chunkCoords,
                                   _cellLPos,
                                   _tupleInputs,
                                   &_tupleValue);
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

    bool end() const
    {
        return _aiters[0]->end();
    }

    Value const* getTuple() const
    {
        if(end())
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Internal inconsistency";
        }
        return _tupleOutput;
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
    vector<Value>                       _outputValues;
    Value                               _boolTrue;

public:
    ArrayWriter(Settings const& settings, shared_ptr<Query> const& query):
        _output               (std::make_shared<MemArray>( MODE==WRITE_OUTPUT ? settings.getOutputSchema() : settings.makeTupledSchema(query), query)),
        _myInstanceId         (query->getInstanceID()),
        _numInstances         (query->getInstancesCount()),
        _numAttributes        (_output->getArrayDesc().getAttributes(true).size()),
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
        _currentDstInstanceId (0),
        _outputValues         (MODE == WRITE_OUTPUT ? _settings.getNumOutputAttrs() : 0)
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

    void writeTuple(Value const* tuple)
    {
        bool newChunk = false;
        if(MODE == WRITE_SPLIT_ON_INSTANCE)
        {
            uint32_t dstInstanceId = RedimTuple::getInstanceId(tuple);
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
            uint32_t dstInstanceId;
            position_t cellPos;
            RedimTuple::decomposeTuple(_settings.getNumOutputDims(),
                                       _settings.getNumOutputAttrs(),
                                       _settings.outputAttributeNullable(),
                                       _settings.getOutputAttributeSizes(),
                                       tuple,
                                       dstInstanceId,
                                       _outputChunkPositionBuf,
                                       cellPos,
                                       _outputValues);
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
        if(MODE==WRITE_OUTPUT)
        {
            for(size_t i=0; i<_numAttributes; ++i)
            {
                _chunkIterators[i]->setPosition(_outputPosition);
                _chunkIterators[i]->writeItem(_outputValues[i]);
            }
        }
        else
        {
            _chunkIterators[0]->setPosition(_outputPosition);
            _chunkIterators[0]->writeItem(*tuple);
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
