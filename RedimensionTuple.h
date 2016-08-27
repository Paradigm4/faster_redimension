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

#ifndef REDIMENSIONTUPLE_H_
#define REDIMENSIONTUPLE_H_

#include "query/Operator.h"
#include "query/FunctionLibrary.h"
#include "query/FunctionDescription.h"
#include "query/TypeSystem.h"

using namespace scidb;
using std::vector;

//Redim tuple:
//tuple:= [uint8 ndims][uint32 instanceId][Coordinate chunkDim1][Coordinate chunkDim2]..[position_t cellPos][value1][value2][...]
//value:= ([int8 missing_code])([uint32 size])([data]) //missing_code only present for nullable; size only present for variable-sized types

class RedimTuple
{
public:
    static void makeRedimTuple(uint8_t const nDims,
                               size_t const nAttrs,
                               vector<bool> const& attrNullable,
                               vector<size_t> const& attrSizes,
                               uint32_t const dstInstanceId,
                               Coordinates const& chunkCoords,
                               position_t const cellPos,
                               vector<Value const*> const& values,
                               Value* redimTuple)
    {
        size_t tupleSize = sizeof(uint8_t) + sizeof(uint32_t) + sizeof(Coordinate)*nDims + sizeof(position_t);
        for(size_t i=0; i<nAttrs; ++i)
        {
            if(attrNullable[i])
            {
                tupleSize += sizeof(int8_t);
                if(values[i]->isNull())
                {
                    continue;
                }
            }
            if(attrSizes[i]!=0) //fixed size
            {
                tupleSize += attrSizes[i];
            }
            else
            {
                tupleSize += sizeof(uint32_t);
                tupleSize += values[i]->size();
            }
        }
        redimTuple->setSize(tupleSize);
        uint8_t* nDimsPtr = reinterpret_cast<uint8_t*>(redimTuple->data());
        *nDimsPtr = nDims;
        ++nDimsPtr;
        uint32_t* iidPtr = reinterpret_cast<uint32_t*>(nDimsPtr);
        *iidPtr = dstInstanceId;
        ++iidPtr;
        Coordinate* coordPtr = reinterpret_cast<Coordinate*>(iidPtr);
        for(size_t i=0; i<nDims; ++i)
        {
            *coordPtr = chunkCoords[i];
            ++coordPtr;
        }
        position_t* posPtr = reinterpret_cast<position_t*>(coordPtr);
        *posPtr = cellPos;
        ++posPtr;
        char* valPtr = reinterpret_cast<char*>(posPtr);
        for(size_t i=0; i<nAttrs; ++i)
        {
            Value const* v = values[i];
            if(attrNullable[i])
            {
                int8_t* mcPtr = reinterpret_cast<int8_t*>(valPtr);
                *mcPtr = v->getMissingReason();
                ++mcPtr;
                valPtr = reinterpret_cast<char*>(mcPtr);
                if(v->isNull())
                {
                    continue;
                }
            }
            if(attrSizes[i]!=0) //fixed size
            {
                memcpy(valPtr, v->data(), attrSizes[i]);
                valPtr += attrSizes[i];
            }
            else
            {
                uint32_t* sizePtr = reinterpret_cast<uint32_t*>(valPtr);
                *sizePtr = v->size();
                ++sizePtr;
                valPtr = reinterpret_cast<char*>(sizePtr);
                memcpy(valPtr, v->data(), v->size());
                valPtr += v->size();
            }
        }
    }

    static uint32_t getInstanceId(Value const* redimTuple)
    {
        uint32_t* iid = reinterpret_cast<uint32_t*>( reinterpret_cast<char*>(redimTuple->data()) + sizeof(uint8_t) );
        return *iid;
    }

    static void setTuplePosition(Value* redimTuple, uint8_t const nDims, position_t const position)
    {
        position_t* posPtr = reinterpret_cast<position_t*>( reinterpret_cast<char*>(redimTuple->data()) + sizeof(uint8_t) + sizeof(uint32_t) + nDims * sizeof(Coordinate));
        *posPtr = position;
    }

    static void decomposeTuple(uint8_t const nDims,
                               size_t const nAttrs,
                               vector<bool> const& attrNullable,
                               vector<size_t> const& attrSizes,
                               Value const* redimTuple,
                               uint32_t& dstInstanceId,
                               Coordinates& chunkCoords,
                               position_t& cellPos,
                               vector<Value>& values)
    {
        uint32_t* iidPtr = reinterpret_cast<uint32_t*>( reinterpret_cast<char*>(redimTuple->data()) + sizeof(uint8_t));
        dstInstanceId = *iidPtr;
        ++iidPtr;
        Coordinate* coordPtr = reinterpret_cast<Coordinate*>(iidPtr);
        for(size_t i =0; i<nDims; ++i)
        {
            chunkCoords[i] = *coordPtr;
            ++coordPtr;
        }
        position_t* posPtr = reinterpret_cast<position_t*>(coordPtr);
        cellPos = *posPtr;
        ++posPtr;
        char* valPtr = reinterpret_cast<char*>(posPtr);
        for(size_t i=0; i<nAttrs; ++i)
        {
            if(attrNullable[i])
            {
                int8_t* mcPtr = reinterpret_cast<int8_t*>(valPtr);
                int8_t mc = *mcPtr;
                ++mcPtr;
                valPtr = reinterpret_cast<char*>(mcPtr);
                if(mc>=0)
                {
                    values[i].setNull(mc);
                    continue;
                }
            }
            if(attrSizes[i]!=0) //fixed size
            {
                values[i].setSize(attrSizes[i]);
                memcpy(values[i].data(), valPtr, attrSizes[i]);
                valPtr += attrSizes[i];
            }
            else
            {
                uint32_t* sizePtr = reinterpret_cast<uint32_t*>(valPtr);
                uint32_t const size = *sizePtr;
                values[i].setSize(size);
                ++sizePtr;
                valPtr = reinterpret_cast<char*>(sizePtr);
                memcpy(values[i].data(), valPtr, size);
                valPtr += size;
            }
        }
    }

    static bool redimTupleLess(Value const* left, Value const* right)
    {
        uint8_t* nDimsL = reinterpret_cast<uint8_t*>(left->data());
        uint8_t* nDimsR = reinterpret_cast<uint8_t*>(right->data());
        uint8_t const numCoords = *nDimsL;
        if(*nDimsR != numCoords)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "malformed tuple comparison";
        }
        nDimsL++;
        nDimsR++;
        uint32_t* instanceL = reinterpret_cast<uint32_t*>(nDimsL);
        uint32_t* instanceR = reinterpret_cast<uint32_t*>(nDimsR);
        if(*instanceL < *instanceR)
        {
            return true;
        }
        if(*instanceL > *instanceR)
        {
            return false;
        }
        ++instanceL;
        ++instanceR;
        Coordinate* coordL = reinterpret_cast<Coordinate*>(instanceL);
        Coordinate* coordR = reinterpret_cast<Coordinate*>(instanceR);
        for(uint8_t i=0; i<numCoords; ++i)
        {
            if(*coordL < *coordR)
            {
                return true;
            }
            else if(*coordL > *coordR)
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

    static bool redimTupleEqual(Value const* left, Value const* right)
    {
        uint8_t* nDimsL = reinterpret_cast<uint8_t*>(left->data());
        uint8_t const nDims = *nDimsL;
        size_t const comparableSize = sizeof(uint8_t) + sizeof(uint32_t) + sizeof(Coordinate) * nDims + sizeof(position_t);
        return (memcmp(left->data(), right->data(), comparableSize) == 0);
    }
};

struct RedimTupleComparator
{
    bool operator() (Value const* i, Value const* j)
    {
        return RedimTuple::redimTupleLess(i, j);
    }
};

#endif /* REDIMENSIONTUPLE_H_ */
