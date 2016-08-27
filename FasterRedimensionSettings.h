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
#include <util/ArrayCoordinatesMapper.h>

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
    size_t const                  _numInstances;
    HashedArrayDistribution const _distribution;
    ArrayCoordinatesMapper  const _mapper;
    size_t                        _numInputAttributesRead;
    vector<size_t>                _inputAttributesRead;
    vector<size_t>                _inputAttributeDestinations; //map into output attributes [0...], then output dimensions [_nOutputAttrs...]
    vector<bool>                  _inputAttributeFilterNull;
    size_t                        _numInputDimensionsRead;
    vector<size_t>                _inputDimensionsRead;
    vector<size_t>                _inputDimensionDestinations;
    vector<size_t>                _outputAttributeSizes;
    vector<bool>                  _outputAttributeNullable;
    size_t                        _estTupleSizeBytes;
    bool                          _estTupleSizeBytesSet;
    size_t                        _sortedArrayChunkSize;
    bool                          _sortedArrayChunkSizeSet;
    size_t                        _sortChunkSizeLimitBytes;
    bool                          _sortChunkSizeLimitBytesSet;
    size_t                        _sgChunkSizeLimitBytes;
    bool                          _sgChunkSizeLimitBytesSet;
    bool                          _haveSynthetic;
    size_t                        _syntheticId;
    Coordinate                    _syntheticMin;
    Coordinate                    _syntheticMax;

    static string paramToString(shared_ptr <OperatorParam> const& parameter, shared_ptr<Query>& query, bool logical)
    {
        if(logical)
        {
            return evaluate(((shared_ptr<OperatorParamLogicalExpression>&) parameter)->getExpression(),query, TID_STRING).getString();
        }
        return ((shared_ptr<OperatorParamPhysicalExpression>&) parameter)->getExpression()->evaluate().getString();
    }

    void setSizeParam (string const& parameterString, bool& alreadySet, string const& header, size_t& param )
    {
        string paramContent = parameterString.substr(header.size());
        if (alreadySet)
        {
            string h = parameterString.substr(0, header.size()-1);
            ostringstream error;
            error<<"illegal attempt to set "<<h<<" multiple times";
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << error.str().c_str();
        }
        trim(paramContent);
        int64_t content;
        try
        {
            content = lexical_cast<int64_t>(paramContent);
        }
        catch (bad_lexical_cast const& exn)
        {
            string h = parameterString.substr(0, header.size()-1);
            ostringstream error;
            error<<"could not parse "<<h;
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << error.str().c_str();
        }
        if(content<=0)
        {
            string h = parameterString.substr(0, header.size()-1);
            ostringstream error;
            error<<h<<" must not be negative";
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << error.str().c_str();
        }
        param = content;
        alreadySet = true;
    }

public:
    static size_t const MAX_PARAMETERS = 5; //1 for the schema

    Settings(ArrayDesc const& inputSchema,
             ArrayDesc const& outputSchema,
             vector< shared_ptr<OperatorParam> > const& operatorParameters,
             bool logical,
             shared_ptr<Query>& query):
        _inputSchema(inputSchema),
        _outputSchema(outputSchema),
        _numInputAttrs(_inputSchema.getAttributes(true).size()),
        _numInputDims(_inputSchema.getDimensions().size()),
        _numOutputAttrs(_outputSchema.getAttributes(true).size()),
        _numOutputDims(_outputSchema.getDimensions().size()),
        _numInstances(query->getInstancesCount()),
        _distribution(0,""),
        _mapper(outputSchema.getDimensions()),
        _numInputAttributesRead(0),
        _inputAttributesRead(0),
        _inputAttributeDestinations(0),
        _inputAttributeFilterNull(0),
        _numInputDimensionsRead(0),
        _inputDimensionsRead(0),
        _inputDimensionDestinations(0),
        _outputAttributeSizes(_numOutputAttrs),
        _outputAttributeNullable(_numOutputAttrs),
        _estTupleSizeBytesSet(false),
        _sortedArrayChunkSizeSet(false),
        _sortChunkSizeLimitBytesSet(false),
        _sgChunkSizeLimitBytesSet(false),
        _haveSynthetic(false),
        _syntheticId(0),
        _syntheticMin(0),
        _syntheticMax(0)
    {
        string const estTupleSizeBytesHeader       = "est_tuple_size_bytes=";        //estimation on how big each tuple is (sort uses this to size the buffer to fit in memory)
        string const sortedChunkSizeHeader         = "sorted_array_chunk_size=";     //chunk size for the output of the sort routine
        string const sortChunkSizeLimitBytesHeader = "sort_chunk_size_limit_bytes="; //limit on chunks that are fed in to sort, not a big deal as long as it's under MERGE_SORT_BUFFER
        string const sgChunkSizeLimitBytesHeader   = "sg_chunk_size_limit_bytes=";   //limit on the chunks that are fed in to post sort SG: a chunk from each instance should fit in memory
        size_t const nParams = operatorParameters.size();
        if (nParams > MAX_PARAMETERS)
        {   //assert-like exception. Caller should have taken care of this!
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "illegal number of parameters passed to faster_redimension";
        }
        for (size_t i= 1; i<nParams; ++i) //first param is the schema, handled elsewhere
        {
          string parameterString = paramToString(operatorParameters[i], query, logical);
          if      (starts_with(parameterString, estTupleSizeBytesHeader))
          {
              setSizeParam(parameterString, _estTupleSizeBytesSet, estTupleSizeBytesHeader, _estTupleSizeBytes);
          }
          else if (starts_with(parameterString, sortedChunkSizeHeader))
          {
              setSizeParam(parameterString, _sortedArrayChunkSizeSet, sortedChunkSizeHeader, _sortedArrayChunkSize);
          }
          else if (starts_with(parameterString, sortChunkSizeLimitBytesHeader))
          {
              setSizeParam(parameterString, _sortChunkSizeLimitBytesSet, sortChunkSizeLimitBytesHeader, _sortChunkSizeLimitBytes);
          }
          else if (starts_with(parameterString, sgChunkSizeLimitBytesHeader))
          {
              setSizeParam(parameterString, _sgChunkSizeLimitBytesSet, sgChunkSizeLimitBytesHeader, _sgChunkSizeLimitBytes);
          }
          else
          {
              ostringstream error;
              error<<"Unrecognized parameter "<<parameterString;
              throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << error.str().c_str();
          }
        }
        mapInputToOutput();
        computeChunkSizes();
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
                if (inputAttr.getName() == outputAttr.getName())
                {
                    _numInputAttributesRead ++;
                    _inputAttributesRead.push_back(i);
                    _inputAttributeDestinations.push_back(j);
                    _inputAttributeFilterNull.push_back(false);
                    found = true;
                }
            }
            for(size_t j=0; j<_numOutputDims && !found; ++j)
            {
                DimensionDesc const& outputDim = _outputSchema.getDimensions()[j];
                if(outputDim.hasNameAndAlias(inputAttr.getName()))
                {
                    _numInputAttributesRead ++;
                    _inputAttributesRead.push_back(i);
                    _inputAttributeDestinations.push_back( _numOutputAttrs + j);
                    _inputAttributeFilterNull.push_back(true);
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
                    _numInputDimensionsRead ++;
                    _inputDimensionsRead.push_back(i);
                    _inputDimensionDestinations.push_back(j);
                    found = true;
                }
            }
            for(size_t j =0; j<_numOutputDims && !found; ++j)
            {
                DimensionDesc const& outputDim = _outputSchema.getDimensions()[j];
                if (inputDim.hasNameAndAlias(outputDim.getBaseName()))
                {
                    _numInputDimensionsRead ++;
                    _inputDimensionsRead.push_back(i);
                    _inputDimensionDestinations.push_back( _numOutputAttrs + j);
                    found = true;
                }
            }
        }
        for(size_t i =0; i<_numOutputAttrs; ++i)
        {
            AttributeDesc const& outputAttr = _outputSchema.getAttributes(true)[i];
            _outputAttributeSizes[i]= outputAttr.getSize();
            _outputAttributeNullable[i] = (outputAttr.getFlags() !=0);
        }
        for(size_t i=0; i<_numOutputDims; ++i)
        {
            DimensionDesc const& outputDim = _outputSchema.getDimensions()[i];
            bool found = false;
            for(size_t j=0; j<_numInputAttrs && !found; ++j)
            {
                AttributeDesc const& inputAttr = _inputSchema.getAttributes(true)[j];
                if (outputDim.hasNameAndAlias(inputAttr.getName()))
                {
                    found = true;
                }
            }
            for(size_t j=0; j<_numInputDims && !found; ++j)
            {
                DimensionDesc const& inputDim = _inputSchema.getDimensions()[j];
                if (inputDim.hasNameAndAlias(outputDim.getBaseName()))
                {
                    found = true;
                }
            }
            if(!found)
            {
                if(_haveSynthetic)
                {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "internal inconsistency";
                }
                _haveSynthetic = true;
                _syntheticId = i;
                _syntheticMin = outputDim.getStartMin();
                _syntheticMax = outputDim.getStartMin() + outputDim.getChunkInterval() - 1;
            }
        }
    }

    void computeChunkSizes()
    {
        if(!_estTupleSizeBytesSet) //Customer's always right!
        {
            _estTupleSizeBytes = computeApproximateTupleSize();
        }
        if(!_sortedArrayChunkSizeSet)
        {
            _sortedArrayChunkSize = (10 * 1024 * 1024) / _estTupleSizeBytes;
            if(_sortedArrayChunkSize < 10)
            {
                _sortedArrayChunkSize = 10;
            }
        }
        size_t const mergeSortBuf = (Config::getInstance()->getOption<int>(CONFIG_MERGE_SORT_BUFFER) * 1024 * 1024);
        if(!_sortChunkSizeLimitBytesSet)
        {
            _sortChunkSizeLimitBytes = mergeSortBuf / 8;
            if (_sortChunkSizeLimitBytes < 128 * 1024)
            {
                _sortChunkSizeLimitBytes = 128*1024;
            }
        }
        if(!_sgChunkSizeLimitBytesSet)
        {
            _sgChunkSizeLimitBytes = mergeSortBuf / _numInstances;
            if(_sgChunkSizeLimitBytes > 10 * 1024 * 1024) //sending messages that are too large may make things unstable
            {
                _sgChunkSizeLimitBytes = 10 * 1024 * 1024;
            }
            else if (_sgChunkSizeLimitBytes < 128 * 1024)
            {
                _sgChunkSizeLimitBytes = 128*1024;
            }
        }
    }

    void logSettings()
    {
        ostringstream output;
        output<<"attributes ";
        for(size_t i=0; i<_numInputAttributesRead; ++i)
        {
            output<<_inputAttributesRead[i]<<" -> "<<_inputAttributeDestinations[i]<<" ";
        }
        output<<" dimensions ";
        for(size_t i=0; i<_numInputDimensionsRead; ++i)
        {
            output<<_inputDimensionsRead[i]<<" -> "<<_inputDimensionDestinations[i]<<" ";
        }
        output<<" synthetic "<<_haveSynthetic<<" id "<<_syntheticId
              <<" synthetic_min "<<_syntheticMin<<" synthetic_max"<<_syntheticMax
              <<" est_tuple_size_bytes="<<_estTupleSizeBytes
              <<" sorted_array_chunk_size="<<_sortedArrayChunkSize
              <<" sort_chunk_size_limit_bytes="<<_sortChunkSizeLimitBytes
              <<" sg_chunk_size_limit_bytes="<<_sgChunkSizeLimitBytes;
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

    size_t getSortedArrayChunkSize() const
    {
        return _sortedArrayChunkSize;
    }

    size_t getNumInputAttributesRead() const
    {
        return _numInputAttributesRead;
    }

    vector<size_t> const& getInputAttributesRead() const
    {
        return _inputAttributesRead;
    }

    vector<size_t> const& getInputAttributeDestinations() const
    {
        return _inputAttributeDestinations;
    }

    vector<bool> const& getInputAttributeFilterNull() const
    {
        return _inputAttributeFilterNull;
    }

    size_t getNumInputDimensionsRead() const
    {
        return _numInputDimensionsRead;
    }

    vector<size_t> const& getInputDimensionsRead() const
    {
        return _inputDimensionsRead;
    }

    vector<size_t> const& getInputDimensionDestinations() const
    {
        return _inputDimensionDestinations;
    }

    uint32_t getInstanceForChunk(Coordinates const& outputChunkPosition) const
    {
        return _distribution.getPrimaryChunkLocation(outputChunkPosition, _outputSchema.getDimensions(), _numInstances);
    }

    void getOutputChunkPosition(Coordinates& outputCellPosition) const
    {
        _outputSchema.getChunkPositionFor(outputCellPosition);
    }

    position_t getOutputCellPos(Coordinates const& outputChunkPosition, Coordinates const& outputCellPosition) const
    {
        return _mapper.coord2pos(outputChunkPosition, outputCellPosition);
    }

    void getOutputCellCoords(Coordinates const& outputChunkPosition, position_t const cellPos, Coordinates& outputCellCoords) const
    {
        _mapper.pos2coord(outputChunkPosition, cellPos, outputCellCoords);
    }

    vector<size_t> const& getOutputAttributeSizes() const
    {
        return _outputAttributeSizes;
    }

    vector<bool> const& outputAttributeNullable() const
    {
        return _outputAttributeNullable;
    }

    size_t getSortChunkSizeLimit() const
    {
        return _sortChunkSizeLimitBytes;
    }

    size_t getSgChunkSizeLimit() const
    {
        return _sgChunkSizeLimitBytes;
    }

    size_t computeApproximateTupleSize() const
    {
        size_t result =  sizeof(uint8_t) + sizeof(uint32_t) + sizeof(Coordinate)*_numOutputDims + sizeof(position_t);
        for(size_t i=0; i<_numOutputAttrs; ++i)
        {
            if(_outputAttributeNullable[i])
            {
                result += sizeof(int8_t);
            }
            if(_outputAttributeSizes[i]!=0) //fixed size
            {
                result += _outputAttributeSizes[i];
            }
            else
            {
                result += sizeof(uint32_t);
                result += Config::getInstance()->getOption<int>(CONFIG_STRING_SIZE_ESTIMATION);
            }
        }
        return result;
    }

    bool haveSynthetic() const
    {
        return _haveSynthetic;
    }

    size_t getSyntheticId() const
    {
        return _syntheticId;
    }

    Coordinate getSyntheticMin() const
    {
        return _syntheticMin;
    }

    Coordinate getSyntheticMax() const
    {
        return _syntheticMax;
    }

    ArrayDesc makePreSortSchema(shared_ptr<Query> const& query, bool includeApproximateTupleSize = false) const
    {
        Attributes outputAttributes(1);
        outputAttributes[0] = AttributeDesc(0, "tuple", "redimension_tuple", 0,0, std::set<std::string>(), NULL, std::string(),
                                            includeApproximateTupleSize ? _estTupleSizeBytes : 0);
        Dimensions outputDimensions;
        outputDimensions.push_back(DimensionDesc("value_no",        0,  CoordinateBounds::getMax(),               _sortedArrayChunkSize,  0));
        return ArrayDesc("redimension_presort" , outputAttributes, outputDimensions, defaultPartitioning(), query->getDefaultArrayResidency());
    }

    ArrayDesc makeSgSchema(shared_ptr<Query> const& query) const
    {
        Attributes outputAttributes(1);
        outputAttributes[0] = AttributeDesc(0, "tuple_set", TID_BINARY, 0,0);
        Dimensions outputDimensions;
        outputDimensions.push_back(DimensionDesc("chunk_no",        0,  CoordinateBounds::getMax(),               1,                       0));
        outputDimensions.push_back(DimensionDesc("dst_instance_id", 0, _numInstances-1,                           1,                       0));
        outputDimensions.push_back(DimensionDesc("src_instance_id", 0, _numInstances-1,                           1,                       0));
        return ArrayDesc("redimension_sg" , outputAttributes, outputDimensions, defaultPartitioning(), query->getDefaultArrayResidency());
    }

    ArrayDesc const& getOutputSchema() const
    {
        return _outputSchema;
    }

};

} } //namespaces

#endif //FASTER_REDIMENSION_SETTINGS

