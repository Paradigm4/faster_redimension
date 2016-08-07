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

#ifndef TUPLEADDRESS_H_
#define TUPLEADDRESS_H_

#include "query/Operator.h"
#include "query/FunctionLibrary.h"
#include "query/FunctionDescription.h"
#include "query/TypeSystem.h"

using scidb::Value;
using scidb::Coordinate;
using scidb::Coordinates;
using scidb::position_t;

static void makeTupleAddress( uint32_t const instanceId, Coordinates const& chunkPos, position_t const cellPos, size_t const nDims, Value* dst)
{
    dst->setSize(sizeof(uint32_t) + nDims * sizeof(Coordinate) + sizeof(uint64_t));
    uint32_t* iid = reinterpret_cast<uint32_t*>(dst->data());
    *iid = instanceId;
    ++iid;
    Coordinate* coord = reinterpret_cast<Coordinate*>(iid);
    for(size_t i=0; i<nDims; ++i)
    {
        *coord = chunkPos[i];
        ++coord;
    }
    position_t* cp = reinterpret_cast<position_t*>(coord);
    *cp = cellPos;
}

static uint32_t getInstanceId(Value const* tupleAddress)
{
    uint32_t* iid = reinterpret_cast<uint32_t*>(tupleAddress->data());
    return *iid;
}

static void getChunkPos(Value const* tupleAddress, size_t const& nDims, Coordinates& chunkPos)
{
    uint32_t* iid = reinterpret_cast<uint32_t*>(tupleAddress->data());
    ++iid;
    Coordinate* coord = reinterpret_cast<Coordinate*>(iid);
    for(size_t i=0; i<nDims; ++i)
    {
        chunkPos[i] = *coord;
        ++coord;
    }
}

static position_t getCellPos(Value const* tupleAddress, size_t const& nDims)
{
    uint32_t* iid = reinterpret_cast<uint32_t*>(tupleAddress->data());
    ++iid;
    Coordinate* coord = reinterpret_cast<Coordinate*>(iid);
    for(size_t i=0; i<nDims; ++i)
    {
        ++coord;
    }
    position_t* cp = reinterpret_cast<position_t*>(coord);
    return *cp;
}

static bool tupleAddressLess(Value const* left, Value const* right)
{
    uint32_t* instanceL = reinterpret_cast<uint32_t*>(left->data());
    uint32_t* instanceR = reinterpret_cast<uint32_t*>(right->data());
    if(*instanceL < *instanceR)
    {
        return true;
    }
    if(*instanceL > *instanceR)
    {
        return false;
    }
    size_t const numCoords =  ((left->size() - sizeof(uint32_t))/sizeof(Coordinate)) - 1;
    ++instanceL;
    ++instanceR;
    Coordinate* coordL = reinterpret_cast<Coordinate*>(instanceL);
    Coordinate* coordR = reinterpret_cast<Coordinate*>(instanceR);
    for(size_t i=0; i<numCoords; ++i)
    {
        if(*coordL < *coordR)
        {
            return true;
        }
        if(*coordL > *coordR)
        {
            return false;
        }
        ++coordL;
        ++coordR;
    }
    position_t *posL = reinterpret_cast<position_t*>(coordL);
    position_t *posR = reinterpret_cast<position_t*>(coordR);
    if(*posL < *posR)
    {
        return true;
    }
    return false;
}

static bool tupleAddressEqual(Value const* left, Value const* right)
{
    return (memcmp(left->data(), right->data(), left->size()) == 0);
}

#endif /* TUPLEADDRESS_H_ */
