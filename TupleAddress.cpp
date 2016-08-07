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
#include "query/FunctionLibrary.h"
#include "query/FunctionDescription.h"
#include "query/TypeSystem.h"

#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/assign.hpp>

using namespace std;
using namespace scidb;
using boost::lexical_cast;
using boost::bad_lexical_cast;
using boost::algorithm::trim;
using namespace boost::assign;


void makeTupleAddress(const Value** args, Value* res, void * v)
{
    res->setNull();
}

void tupleAddressToString(const Value** args, Value* res, void * v)
{
    Value const* input = args[0];
    if(input->isNull())
    {
        res->setNull(input->getMissingReason());
        return;
    }
    if(input->size() == 0)
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "zero-sized address provided";
    }
    if( (input->size() - sizeof(uint32_t))% sizeof(int64_t) !=0)
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "improperly sized address provided";
    }
    size_t const numCoords = ((input->size()-sizeof(uint32_t))/sizeof(int64_t)) -1;
    ostringstream output;
    uint32_t* iid = reinterpret_cast<uint32_t*>(input->data());
    output << *iid<<"|";
    ++iid;
    Coordinate* coord = reinterpret_cast<Coordinate*>(iid);
    for(size_t i=0; i<numCoords; ++i)
    {
        output<<*coord<<"|";
        ++coord;
    }
    uint64_t* chunkPos = reinterpret_cast<uint64_t*>(coord);
    output<<*chunkPos;
    res->setString(output.str());
}


void stringToTupleAddress (const scidb::Value** args, scidb::Value* res, void*)
{
    if(args[0]->isNull())
    {
        res->setNull(args[0]->getMissingReason());
        return;
    }
    string input(args[0]->getString());
    trim(input);
    char delimiter='|';
    stringstream ss(input); // Turn the string into a stream.
    string tok;
    if(!getline(ss, tok, delimiter))
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "improper string provided";
    }
    uint32_t instanceId = lexical_cast<uint32_t>(tok);
    vector<int64_t> data;
    while(getline(ss, tok, delimiter))
    {
        data.push_back(lexical_cast<int64_t>(tok));
    }
    res->setSize( data.size()*sizeof(int64_t) + sizeof(uint32_t) );
    uint32_t* iidPtr = reinterpret_cast<uint32_t*>(res->data());
    *iidPtr = instanceId;
    ++iidPtr;
    int64_t* dataPtr = reinterpret_cast<int64_t*>(iidPtr);
    for(size_t i=0; i<data.size(); ++i)
    {
        *dataPtr = data[i];
        ++dataPtr;
    }
}

void tupleAddressLessThan(const scidb::Value** args, scidb::Value* res, void*)
{
    if(args[0]->isNull() || args[1]->isNull())
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "baby don't call me on null inputs";
    }
    if(args[0]->size() != args[1]->size() || args[0]->size() == 0)
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "baby don't call me on uneven addresses";
    }
    uint32_t* instanceL = reinterpret_cast<uint32_t*>(args[0]->data());
    uint32_t* instanceR = reinterpret_cast<uint32_t*>(args[1]->data());
    if(*instanceL < *instanceR)
    {
        res->setBool(true);
        return;
    }
    if(*instanceL > *instanceR)
    {
        res->setBool(false);
        return;
    }
    size_t const numCoords =  ((args[0]->size() - sizeof(uint32_t))/sizeof(Coordinate)) - 1;
    ++instanceL;
    ++instanceR;
    Coordinate* coordL = reinterpret_cast<Coordinate*>(instanceL);
    Coordinate* coordR = reinterpret_cast<Coordinate*>(instanceR);
    for(size_t i=0; i<numCoords; ++i)
    {
        if(*coordL < *coordR)
        {
            res->setBool(true);
            return;
        }
        if(*coordL > *coordR)
        {
            res->setBool(false);
            return;
        }
        ++coordL;
        ++coordR;
    }
    uint64_t *posL = reinterpret_cast<uint64_t*>(coordL);
    uint64_t *posR = reinterpret_cast<uint64_t*>(coordR);
    if(*posL < *posR)
    {
        res->setBool(true);
        return;
    }
    res->setBool(false);
}

void tupleAddressEqual(const scidb::Value** args, scidb::Value* res, void*)
{
    if(args[0]->isNull() || args[1]->isNull())
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "baby don't call me on null inputs";
    }
    if(args[0]->size() != args[1]->size() || args[0]->size() == 0)
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "baby don't call me on uneven addresses";
    }
    res->setBool(memcmp(args[0]->data(), args[1]->data(), args[0]->size()) == 0);
}


REGISTER_TYPE(tuple_address, 0);

//Default ctor
REGISTER_FUNCTION(tuple_address, ArgTypes(), "tuple_address", makeTupleAddress);

//To and from string
REGISTER_FUNCTION(tuple_address_to_string, list_of("tuple_address"), TID_STRING, tupleAddressToString);
REGISTER_CONVERTER(tuple_address, string, EXPLICIT_CONVERSION_COST, tupleAddressToString);
REGISTER_FUNCTION(string_to_tuple_address, list_of(TID_STRING), "tuple_address", stringToTupleAddress);
REGISTER_CONVERTER(string, tuple_address, EXPLICIT_CONVERSION_COST, stringToTupleAddress);

//Comparisons
REGISTER_FUNCTION(<, list_of("tuple_address")("tuple_address"), TID_BOOL, tupleAddressLessThan);
REGISTER_FUNCTION(=, list_of("tuple_address")("tuple_address"), TID_BOOL, tupleAddressEqual);
