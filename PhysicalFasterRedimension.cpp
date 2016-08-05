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

    shared_ptr<Array> sortArray(shared_ptr<Array> & tupledArray, shared_ptr<Query>& query, Settings const& settings, bool pre)
    {
        arena::ArenaPtr sortArena = _arena;
        if(Config::getInstance()->getOption<int>(CONFIG_RESULT_PREFETCH_QUEUE_SIZE) == 1)
        {
            arena::Options options;
            options.name  ("FR sort");
            options.parent(_arena);
            options.threading(false);
            options.resetting(true);
            options.pagesize(8*1024*1024);
            sortArena = arena::newArena(options);
        }
        size_t const numSortedFields = pre ? 1 : settings.getNumOutputDims() * 2;
        SortingAttributeInfos sortingAttributeInfos(numSortedFields);
        if(pre)
        {
            sortingAttributeInfos[0].columnNo = 0;
            sortingAttributeInfos[0].ascent = true;
        }
        else
        {
            for(size_t i=0; i<numSortedFields; ++i)
            {
                sortingAttributeInfos[i].columnNo = i+1;
                sortingAttributeInfos[i].ascent = true;
            }
        }
        SortArray sorter(tupledArray->getArrayDesc(), sortArena, false, settings.getTupledChunkSize());
        shared_ptr<TupleComparator> tcomp(make_shared<TupleComparator>(sortingAttributeInfos, tupledArray->getArrayDesc()));
        return sorter.getSortedArray(tupledArray, query, tcomp);
    }

    shared_ptr< Array> execute(vector< shared_ptr< Array> >& inputArrays, shared_ptr<Query> query)
    {
        ArrayDesc const& inputSchema = inputArrays[0]->getArrayDesc();
        Settings settings(inputSchema, _schema, query);
        shared_ptr<Array>& inputArray = inputArrays[0];
        inputArray = arrayPass<READ_INPUT, WRITE_TUPLED>            (inputArray, query, settings);
        inputArray = sortArray(inputArray, query, settings, true);
        inputArray = arrayPass<READ_TUPLED, WRITE_SPLIT_ON_INSTANCE>(inputArray, query, settings);
        inputArray = redistributeToRandomAccess(inputArray, createDistribution(psByCol),query->getDefaultArrayResidency(), query, true);
        inputArray = sortArray(inputArray, query, settings, false);
        return arrayPass<READ_TUPLED, WRITE_OUTPUT>(inputArray, query, settings);


        //return shared_ptr<Array>(new MemArray(_schema, query));
    }
};

REGISTER_PHYSICAL_OPERATOR_FACTORY(PhysicalFasterRedimension, "faster_redimension", "physical_faster_redimension");
} //namespace scidb
