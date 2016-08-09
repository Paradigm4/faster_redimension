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


#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/assign.hpp>
#include "RedimensionTuple.h"

using namespace std;
using namespace scidb;
using boost::lexical_cast;
using boost::bad_lexical_cast;
using boost::algorithm::trim;
using namespace boost::assign;

void RedimensionTuple_makeRedimTuple(const Value** args, Value* res, void * v)
{
    res->setNull();
}

void RedimensionTuple_redimTupleLess(const Value** args, Value* res, void * v)
{
    if(args[0]->isNull() || args[1]->isNull())
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "baby don't call me on null inputs";
    }
    if(args[0]->size() == 0 || args[1]->size() == 0)
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "baby don't call me on uneven addresses";
    }
    res->setBool( RedimTuple::redimTupleLess(args[0],args[1]));
}

void RedimensionTuple_redimTupleEqual(const Value** args, Value* res, void * v)
{
    if(args[0]->isNull() || args[1]->isNull())
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "baby don't call me on null inputs";
    }
    if(args[0]->size() == 0 || args[1]->size() == 0)
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "baby don't call me on uneven addresses";
    }
    res->setBool( RedimTuple::redimTupleEqual(args[0],args[1]));
}

REGISTER_TYPE(redimension_tuple, 0);

//Default ctor
REGISTER_FUNCTION(redimension_tuple, ArgTypes(), "redimension_tuple", RedimensionTuple_makeRedimTuple);
//Comparisons
REGISTER_FUNCTION(<, list_of("redimension_tuple")("redimension_tuple"), TID_BOOL, RedimensionTuple_redimTupleLess);
REGISTER_FUNCTION(=, list_of("redimension_tuple")("redimension_tuple"), TID_BOOL, RedimensionTuple_redimTupleEqual);


