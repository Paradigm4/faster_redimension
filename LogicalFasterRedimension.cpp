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

namespace scidb
{

using namespace std;

class LogicalFastRedim : public LogicalOperator
{
public:
    LogicalFastRedim(const string& logicalName, const string& alias):
        LogicalOperator(logicalName, alias)
    {
        ADD_PARAM_INPUT();
        ADD_PARAM_SCHEMA();
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

        //Ensure attributes names uniqueness.
        size_t numPreservedAttributes = 0;
        for (const AttributeDesc &dstAttr : dstDesc.getAttributes())
        {
            // Look for dstAttr among the source and aggregate attributes.
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

            // Not among the source attributes, look for it among source dimensions (copied to
            // aggregationDesc above).
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

            // This dstAttr should now be accounted for (i.e. we know where it's derived from), so
            // if we get here then this one better be the emptyBitmap.
            if (dstAttr.isEmptyIndicator() == false)
            {
                throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_UNEXPECTED_DESTINATION_ATTRIBUTE)
                << dstAttr.getName();
            }
        NextAttr:;
        }

        // Similarly, make sure we know how each dstDim is derived.
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
                        // (TID_UINT64 is the only integral type that won't safely convert to TID_INT64.)
                        throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_WRONG_SOURCE_ATTRIBUTE_TYPE)
                            << srcAttr.getName();
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

        return ArrayDesc(srcDesc.getName(),
                         dstDesc.getAttributes(),
                         outputDims,
                         createDistribution(psUndefined),
                         query->getDefaultArrayResidency(),
                         dstDesc.getFlags());
    }
};

REGISTER_LOGICAL_OPERATOR_FACTORY(LogicalFastRedim, "faster_redimension");

}
