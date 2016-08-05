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

#ifndef FASTER_REDIMENSION_SETTINGS
#define FASTER_REDIMENSION_SETTINGS

#include <query/Operator.h>
#include <query/Expression.h>
#include <query/AttributeComparator.h>
#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>

namespace scidb
{

namespace faster_redimension
{

using std::string;
using std::vector;
using std::shared_ptr;
using std::dynamic_pointer_cast;
using std::ostringstream;
using std::stringstream;
using boost::algorithm::trim;
using boost::starts_with;
using boost::lexical_cast;
using boost::bad_lexical_cast;

// Logger for operator. static to prevent visibility of variable outside of file
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.operators.faster_redimension"));

class Settings
{
private:
    ArrayDesc const               _inputSchema;
    ArrayDesc const               _outputSchema;
    size_t const                  _numInputAttrs;  //minus empty tag
    size_t const                  _numInputDims;
    size_t const                  _numOutputAttrs; //minus empty tag
    size_t const                  _numOutputDims;
    size_t const                  _tupleSize;    //dst instance id + num output dims * 2 + output attrs
    size_t const                  _tupledArrayChunkSize;
    size_t const                  _numInstances;
    HashedArrayDistribution const _distribution;
    vector<ssize_t>               _mapToTuple;   //one index for each input attr, followed by one index for each input dim

public:
    static size_t const MAX_PARAMETERS = 11;

    Settings(ArrayDesc const& inputSchema,
             ArrayDesc const& outputSchema,
             shared_ptr<Query>& query):
        _inputSchema(inputSchema),
        _outputSchema(outputSchema),
        _numInputAttrs(_inputSchema.getAttributes(true).size()),
        _numInputDims(_inputSchema.getDimensions().size()),
        _numOutputAttrs(_outputSchema.getAttributes(true).size()),
        _numOutputDims(_outputSchema.getDimensions().size()),
        _tupleSize( 1 + _numOutputDims * 2 + _numOutputAttrs),
        _tupledArrayChunkSize( 1000000 / ((_numOutputAttrs + _numOutputDims + 10)/10)), //slightly reduce for large numbers of attributes/dimensions
        _numInstances(query->getInstancesCount()),
        _distribution(0,""),
        _mapToTuple(_numInputAttrs + _numInputDims, -1)
    {
        mapInputToOutput();
        logSettings();
    }

private:
    void throwIf(bool const cond, char const* errorText)
    {
        if(cond)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << errorText;
        }
    }

    void mapInputToOutput()
    {
        for(size_t i=0; i<_numInputAttrs; ++i)
        {
            AttributeDesc const& inputAttr = _inputSchema.getAttributes(true)[i];
            bool found = false;
            for(size_t j =0; j<_numOutputAttrs && !found; ++j)
            {
                AttributeDesc const& outputAttr = _outputSchema.getAttributes(true)[j];
                if (inputAttr.getName() == inputAttr.getName())
                {
                    _mapToTuple[i] = j + 1 + _numOutputDims * 2;
                    found = true;
                }
            }
            for(size_t j=0; j<_numOutputDims && !found; ++j)
            {
                DimensionDesc const& outputDim = _outputSchema.getDimensions()[j];
                if(outputDim.hasNameAndAlias(inputAttr.getName()))
                {
                    _mapToTuple[i] = j + 1 + _numOutputDims;
                    found =true;
                }
            }
        }
        for(size_t i=0; i<_numInputDims; ++i)
        {
            DimensionDesc const& inputDim = _inputSchema.getDimensions()[i];
            bool found = false;
            for(size_t j =0; j<_numOutputAttrs && !found; ++j)
            {
                AttributeDesc const& outputAttr = _outputSchema.getAttributes(true)[j];
                if (inputDim.hasNameAndAlias(outputAttr.getName()))
                {
                    _mapToTuple[i + _numInputAttrs] = j + 1 + _numOutputDims * 2;
                    found = true;
                }
            }
            for(size_t j =0; j<_numOutputDims && !found; ++j)
            {
                DimensionDesc const& outputDim = _outputSchema.getDimensions()[j];
                if (inputDim.hasNameAndAlias(outputDim.getBaseName()))
                {
                    _mapToTuple[i + _numInputAttrs] = j + 1 + _numOutputDims;
                    found = true;
                }
            }
        }
    }

    void logSettings()
    {
        ostringstream output;
        for(size_t i=0; i<_numInputAttrs+_numInputDims; ++i)
        {
            output<<i<<" -> "<<_mapToTuple[i]<<" ";
        }
        output<<" tchunk "<<_tupledArrayChunkSize;
        LOG4CXX_DEBUG(logger, "FR tuple mapping "<<output.str().c_str());
    }

public:
    size_t getNumInputAttrs() const
    {
        return _numInputAttrs;
    }

    size_t getNumInputDims() const
    {
        return _numInputDims;
    }

    size_t getNumOutputAttrs() const
    {
        return _numOutputAttrs;
    }

    size_t getNumOutputDims() const
    {
        return _numOutputDims;
    }

    size_t getTupledChunkSize() const
    {
        return _tupledArrayChunkSize;
    }

    bool isInputFieldUsed(size_t const idx)
    {
        return _mapToTuple[idx] !=-1;
    }

    bool isInputFieldMappedToDimension(size_t const idx)
    {
        return _mapToTuple[idx] !=-1 && _mapToTuple[idx] < static_cast<ssize_t>(1 + _numOutputDims * 2);
    }

    void getOutputChunkPosition(Coordinates& outputCellPosition)
    {
        _outputSchema.getChunkPositionFor(outputCellPosition);
    }

    uint32_t getInstanceForChunk(Coordinates const& outputChunkPosition)
    {
        return _distribution.getPrimaryChunkLocation(outputChunkPosition, _outputSchema.getDimensions(), _numInstances);
    }

    ArrayDesc makeTupledSchema(shared_ptr<Query> const& query)
    {
        Attributes outputAttributes(_tupleSize);
        outputAttributes[0] = AttributeDesc(0, "dst_instance", TID_UINT32, 0,0);
        AttributeID att = 1;
        for(size_t i =0; i<_numOutputDims; ++i)
        {
            outputAttributes[att] = AttributeDesc(att, "chunk_pos", TID_INT64, 0,0);
            ++att;
        }
        for(size_t i =0; i<_numOutputDims; ++i)
        {
            outputAttributes[att] = AttributeDesc(att, "cell_pos", TID_INT64, 0,0);
            ++att;
        }
        Attributes const& attrs = _outputSchema.getAttributes(true);
        for(size_t i =0; i<_numOutputAttrs; ++i)
        {
            outputAttributes[att] = AttributeDesc(att, attrs[i].getName(), attrs[i].getType(), attrs[i].getFlags(), attrs[i].getDefaultCompressionMethod());
        }
        outputAttributes = addEmptyTagAttribute(outputAttributes);
        Dimensions outputDimensions;
        outputDimensions.push_back(DimensionDesc("value_no",        0,  CoordinateBounds::getMax(),               _tupledArrayChunkSize,  0));
        outputDimensions.push_back(DimensionDesc("dst_instance_id", 0, _numInstances,                            1,                       0));
        outputDimensions.push_back(DimensionDesc("src_instance_id", 0, _numInstances,                            1,                       0));
        return ArrayDesc("equi_join_state" , outputAttributes, outputDimensions, defaultPartitioning(), query->getDefaultArrayResidency());
    }

};

} } //namespaces

#endif //FASTER_REDIMENSION_SETTINGS

