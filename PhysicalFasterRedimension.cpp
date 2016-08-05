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

//    shared_ptr<Array> sortArray(shared_ptr<Array> & inputArray, shared_ptr<Query>& query, Settings const& settings)
//    {
//        SortingAttributeInfos sortingAttributeInfos(settings.getNumKeys() + 1); //plus hash
//        sortingAttributeInfos[0].columnNo = inputArray->getArrayDesc().getAttributes(true).size()-1;
//        sortingAttributeInfos[0].ascent = true;
//        for(size_t k=0; k<settings.getNumKeys(); ++k)
//        {
//            sortingAttributeInfos[k+1].columnNo = k;
//            sortingAttributeInfos[k+1].ascent = true;
//        }
//        SortArray sorter(inputArray->getArrayDesc(), _arena, false, settings.getChunkSize());
//        shared_ptr<TupleComparator> tcomp(make_shared<TupleComparator>(sortingAttributeInfos, inputArray->getArrayDesc()));
//        return sorter.getSortedArray(inputArray, query, tcomp);
//    }

    shared_ptr< Array> execute(vector< shared_ptr< Array> >& inputArrays, shared_ptr<Query> query)
    {
        ArrayDesc const& inputSchema = inputArrays[0]->getArrayDesc();
        Settings settings(inputSchema, _schema, query);
        ArrayReader reader(inputArrays[0], settings);
        size_t const tupleSize = settings.getTupleSize();
        while(!reader.end())
        {
            vector<Value const*> tuple = reader.getTuple();
            ostringstream tupleText;
            for(size_t i=0; i<tupleSize; ++i)
            {
                tupleText<<tuple[i]->getInt64()<<" ";
            }
            LOG4CXX_DEBUG(logger, "FR tuple "<<tupleText.str().c_str());
            reader.next();
        }
        return shared_ptr<Array>(new MemArray(_schema, query));
    }
};

REGISTER_PHYSICAL_OPERATOR_FACTORY(PhysicalFasterRedimension, "faster_redimension", "physical_faster_redimension");
} //namespace scidb
