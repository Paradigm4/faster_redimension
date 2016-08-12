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

#include "query/Operator.h"
#include "FasterRedimensionSettings.h"

namespace scidb
{

using namespace std;
using faster_redimension::Settings;

class LogicalFastRedim : public LogicalOperator
{
public:
    LogicalFastRedim(const string& logicalName, const string& alias):
        LogicalOperator(logicalName, alias)
    {
        ADD_PARAM_INPUT();
        ADD_PARAM_SCHEMA();
        ADD_PARAM_VARIES();
    }

    std::vector<shared_ptr<OperatorParamPlaceholder> > nextVaryParamPlaceholder(const std::vector< ArrayDesc> &schemas)
    {
        std::vector<shared_ptr<OperatorParamPlaceholder> > res;
        res.push_back(END_OF_VARIES_PARAMS());
        if (_parameters.size() < Settings::MAX_PARAMETERS)
        {
            res.push_back(PARAM_CONSTANT("string"));
        }
        return res;
    }

    ArrayDesc inferSchema(vector< ArrayDesc> schemas, shared_ptr< Query> query)
    {
        assert(schemas.size() == 1);
        ArrayDesc const& srcDesc = schemas[0];
        ArrayDesc dstDesc = ((std::shared_ptr<OperatorParamSchema>&)_parameters[0])->getSchema();
        if (!dstDesc.getEmptyBitmapAttribute())
        {
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_REDIMENSION_ERROR1);
        }
        size_t numPreservedAttributes = 0;
        for (const AttributeDesc &dstAttr : dstDesc.getAttributes())
        {
            for (const AttributeDesc &srcAttr : srcDesc.getAttributes())
            {
                if (srcAttr.getName() == dstAttr.getName())
                {
                    if (srcAttr.getType() != dstAttr.getType())
                    {
                        throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_WRONG_ATTRIBUTE_TYPE)
                        << srcAttr.getName() << srcAttr.getType() << dstAttr.getType();
                    }
                    if (!dstAttr.isNullable() && srcAttr.isNullable())
                    {
                        throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_WRONG_ATTRIBUTE_FLAGS)
                        << srcAttr.getName();
                    }
                    if (!srcAttr.isEmptyIndicator())
                    {
                        ++numPreservedAttributes;
                    }
                    goto NextAttr;
                }
            }
            for (const DimensionDesc &srcDim : srcDesc.getDimensions())
            {
                if (srcDim.hasNameAndAlias(dstAttr.getName()))
                {
                    if (dstAttr.getType() != TID_INT64)
                    {
                        throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_WRONG_DESTINATION_ATTRIBUTE_TYPE)
                        << dstAttr.getName() << TID_INT64;
                    }
                    goto NextAttr;
                }
            }
            if (dstAttr.isEmptyIndicator() == false)
            {
                throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_UNEXPECTED_DESTINATION_ATTRIBUTE)
                << dstAttr.getName();
            }
        NextAttr:;
        }
        Dimensions outputDims;
        size_t nNewDims = 0;
        for (const DimensionDesc &dstDim : dstDesc.getDimensions())
        {
            int64_t interval = dstDim.getChunkIntervalIfAutoUse(std::max(1L, dstDim.getChunkOverlap()));
            if (dstDim.getChunkOverlap() > interval)
            {
                throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OVERLAP_CANT_BE_LARGER_CHUNK);
            }
            for (const AttributeDesc &srcAttr : srcDesc.getAttributes())
            {
                if (dstDim.hasNameAndAlias(srcAttr.getName()))
                {
                    if ( !IS_INTEGRAL(srcAttr.getType())  || srcAttr.getType() == TID_UINT64 )
                    {
                        throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_WRONG_SOURCE_ATTRIBUTE_TYPE) << srcAttr.getName();
                    }
                    outputDims.push_back(dstDim);
                    goto NextDim;
                }
            }
            for (const DimensionDesc &srcDim : srcDesc.getDimensions())
            {
                if (srcDim.hasNameAndAlias(dstDim.getBaseName()))
                {
                    DimensionDesc outputDim = dstDim;
                    outputDims.push_back(outputDim);
                    goto NextDim;
                }
            }
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "no extraneous dimensions allowed";
        NextDim:;
        }
        ArrayDesc outSchema(srcDesc.getName(),
                            dstDesc.getAttributes(),
                            outputDims,
                            createDistribution(psUndefined),
                            query->getDefaultArrayResidency(),
                            dstDesc.getFlags());
        Settings settings(srcDesc, outSchema, _parameters, true, query);
        return outSchema;
    }
};

REGISTER_LOGICAL_OPERATOR_FACTORY(LogicalFastRedim, "faster_redimension");

}
