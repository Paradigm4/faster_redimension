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


////void makeTupleAddress(const Value** args, Value* res, void * v)
////{
////    res->setNull();
////}
////
////void tupleAddressToString(const Value** args, Value* res, void * v)
////{
////    Value const* input = args[0];
////    if(input->isNull())
////    {
////        res->setNull(input->getMissingReason());
////        return;
////    }
////    if(input->size() == 0)
////    {
////        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "zero-sized address provided";
////    }
////    if( (input->size() - sizeof(uint32_t))% sizeof(int64_t) !=0)
////    {
////        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "improperly sized address provided";
////    }
////    size_t const numCoords = ((input->size()-sizeof(uint32_t))/sizeof(int64_t)) -1;
////    ostringstream output;
////    uint32_t* iid = reinterpret_cast<uint32_t*>(input->data());
////    output << *iid<<"|";
////    ++iid;
////    Coordinate* coord = reinterpret_cast<Coordinate*>(iid);
////    for(size_t i=0; i<numCoords; ++i)
////    {
////        output<<*coord<<"|";
////        ++coord;
////    }
////    position_t* cellPos = reinterpret_cast<position_t*>(coord);
////    output<<*cellPos;
////    res->setString(output.str());
////}
////
////
////void stringToTupleAddress (const scidb::Value** args, scidb::Value* res, void*)
////{
////    if(args[0]->isNull())
////    {
////        res->setNull(args[0]->getMissingReason());
////        return;
////    }
////    string input(args[0]->getString());
////    trim(input);
////    char delimiter='|';
////    stringstream ss(input); // Turn the string into a stream.
////    string tok;
////    if(!getline(ss, tok, delimiter))
////    {
////        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "improper string provided";
////    }
////    uint32_t instanceId = lexical_cast<uint32_t>(tok);
////    vector<int64_t> data;
////    while(getline(ss, tok, delimiter))
////    {
////        data.push_back(lexical_cast<int64_t>(tok));
////    }
////    res->setSize( data.size()*sizeof(int64_t) + sizeof(uint32_t) );
////    uint32_t* iidPtr = reinterpret_cast<uint32_t*>(res->data());
////    *iidPtr = instanceId;
////    ++iidPtr;
////    int64_t* dataPtr = reinterpret_cast<int64_t*>(iidPtr);
////    for(size_t i=0; i<data.size(); ++i)
////    {
////        *dataPtr = data[i];
////        ++dataPtr;
////    }
////}
//
//void tupleAddressLess(const scidb::Value** args, scidb::Value* res, void*)
//{
//    if(args[0]->isNull() || args[1]->isNull())
//    {
//        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "baby don't call me on null inputs";
//    }
//    if(args[0]->size() != args[1]->size() || args[0]->size() == 0)
//    {
//        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "baby don't call me on uneven addresses";
//    }
//    res->setBool(tupleAddressLess(args[0], args[1]));
//}
//
//void tupleAddressEqual(const scidb::Value** args, scidb::Value* res, void*)
//{
//    if(args[0]->isNull() || args[1]->isNull())
//    {
//        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "baby don't call me on null inputs";
//    }
//    if(args[0]->size() != args[1]->size() || args[0]->size() == 0)
//    {
//        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "baby don't call me on uneven addresses";
//    }
//    res->setBool(tupleAddressEqual(args[0], args[1]));
//}
//
//REGISTER_TYPE(tuple_address, 0);
//
////Default ctor
//REGISTER_FUNCTION(tuple_address, ArgTypes(), "tuple_address", makeTupleAddress);
//
////To and from string
//REGISTER_FUNCTION(tuple_address_to_string, list_of("tuple_address"), TID_STRING, tupleAddressToString);
//REGISTER_CONVERTER(tuple_address, string, EXPLICIT_CONVERSION_COST, tupleAddressToString);
//REGISTER_FUNCTION(string_to_tuple_address, list_of(TID_STRING), "tuple_address", stringToTupleAddress);
//REGISTER_CONVERTER(string, tuple_address, EXPLICIT_CONVERSION_COST, stringToTupleAddress);
//
////Comparisons
//REGISTER_FUNCTION(<, list_of("tuple_address")("tuple_address"), TID_BOOL, tupleAddressLess);
//REGISTER_FUNCTION(=, list_of("tuple_address")("tuple_address"), TID_BOOL, tupleAddressEqual);

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


