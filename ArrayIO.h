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

namespace scidb
{
namespace faster_redimension
{

enum ReadArrayType
{
    READ_INPUT,          //we're reading the input array, however it may be; we reorder attributes and convert dimensions to tuple if needed;
                         //here we filter dimensions for nulls
    READ_TUPLED,         //we're reading an array that's been tupled (see above schema)
};

//template<ReadArrayType MODE>
class ArrayReader
{
private:
    shared_ptr<Array>                       _input;
    Settings const&                         _settings;
    vector<Value const*>                    _tuple;                 //external: corresponds to the left or right tuple desired
    size_t const                            _nOutputDims;
    Coordinates                             _chunkPos;
    vector<Value>                           _chunkPosVals;
    Value                                   _instanceIdVal;
    size_t                                  _nReadAttrs;            //internal: corresponds to num attributes we're actually reading
    size_t                                  _nIters;               //number of array iterators (at least 1 even if not reading attributes)
    vector<size_t>                          _readAttrs;             //indeces of the attributes we're reading;
    vector<size_t>                          _readAttrDestinations;  //map each attribute we're reading to tuple
    vector<bool>                            _readAttrFilterNull;    //true if the respective attribute should be filtered for NULL
    size_t                                  _nReadDims;
    vector<size_t>                          _readDims;              //indece of the dimensions we're reading
    vector<size_t>                          _readDimDestinations;   //map each dimension we're reading to tuple
    vector<Value>                           _dimVals;               //values of dimensions we're reading
    vector<shared_ptr<ConstArrayIterator> > _aiters;
    vector<shared_ptr<ConstChunkIterator> > _citers;

public:
    ArrayReader( shared_ptr<Array>& input, Settings const& settings):
        _input(input),
        _settings(settings),
        _tuple(settings.getTupleSize()),
        _nOutputDims(settings.getNumOutputDims()),
        _chunkPos(_nOutputDims),
        _chunkPosVals(_nOutputDims)
    {
        size_t const numInputAttrs = settings.getNumInputAttrs();
        _nReadAttrs =0;
        for(size_t i=0; i<numInputAttrs; ++i)
        {
            if(!settings.isInputFieldUsed(i))
            {
                continue;
            }
            ++_nReadAttrs;
            _readAttrs.push_back(i);
            _readAttrDestinations.push_back( settings.mapInputFieldToTuple(i));
            _readAttrFilterNull.push_back( settings.isInputFieldMappedToDimension(i));
        }
        _nReadDims=0;
        size_t const numInputDims = settings.getNumInputDims();
        for(size_t i =0; i<numInputDims; ++i)
        {
            if(!settings.isInputFieldUsed( i + numInputAttrs))
            {
                continue;
            }
            ++_nReadDims;
            _readDims.push_back(i);
            _readDimDestinations.push_back(settings.mapInputFieldToTuple(i + numInputAttrs));
        }
        _dimVals.resize(_nReadDims);
        if(_nReadAttrs ==0 )
        {
            _nIters = 1;
        }
        else
        {
            _nIters = _nReadAttrs;
        }
        _aiters.resize(_nIters);
        _citers.resize(_nIters);
        for(size_t i =0; i<_nIters; ++i)
        {
            if(_nReadAttrs ==0)
            {
                _aiters[i] = _input->getConstIterator(numInputAttrs); //empty tag
            }
            else
            {
                _aiters[i] = _input->getConstIterator(_readAttrs[i]);
            }
        }
        _tuple[0] = &_instanceIdVal;
        for(size_t i=0; i<_nOutputDims; ++i)
        {
            _tuple[i+1] = &_chunkPosVals[i];
        }
        for(size_t i =0; i<_nReadDims; ++i)
        {
            size_t idx = _readDimDestinations[i];
            _tuple [ idx ] = &_dimVals[i];
        }
        if(!end())
        {
            next<true>();
        }
    }

private:
    bool setAndCheckTuple()
    {
        for(size_t i =0; i<_nReadAttrs; ++i)
        {
            size_t idx = _readAttrDestinations[i];
            _tuple[idx] = &(_citers[i]->getItem());
            if (_readAttrFilterNull[i] && _tuple[idx]->isNull())  //filter for NULLs
            {
                return false;
            }
        }
        Coordinates const& pos = _citers[0]->getPosition();
        for(size_t i =0; i<_nReadDims; ++i)
        {
            Coordinate c = pos[_readDims[i]];
            _dimVals[i].setInt64(c);
            size_t idx = _readDimDestinations[i];
            _tuple [ idx ] = &_dimVals[i];
        }
        for(size_t i=0; i<_nOutputDims; ++i)
        {
            _chunkPos[i] = _tuple[ 1 + _nOutputDims + i]->getInt64();
        }
        _settings.getOutputChunkPosition(_chunkPos);
        for(size_t i=0; i<_nOutputDims; ++i)
        {
            _chunkPosVals[i].setInt64(_chunkPos[i]);
        }
        _instanceIdVal.setUint32(_settings.getInstanceForChunk(_chunkPos));
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
            for(size_t i =0; i<_nIters; ++i)
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
            for(size_t i =0; i<_nIters; ++i)
            {
                ++(*_citers[i]);
            }
            if(findNextTupleInChunk())
            {
                return;
            }
            for(size_t i =0; i<_nIters; ++i)
            {
                ++(*_aiters[i]);
            }
        }
        while(!_aiters[0]->end())
        {
            for(size_t i =0; i<_nIters; ++i)
            {
                _citers[i] = _aiters[i]->getChunk().getConstIterator();
            }
            if(findNextTupleInChunk())
            {
                return;
            }
            for(size_t i =0; i<_nIters; ++i)
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

} } //namespaces




#endif /* ARRAYIO_H_ */
