#include "Hashing.h"

using namespace std;

uint32_t FNV32Hashing::FNV_32_INIT = 2166136261UL;
uint32_t FNV32Hashing::FNV_32_PRIME = 16777619;

double FNV32Hashing::get_point (string key)
{
    uint32_t hash;
    /* this is a modified fvn algorithm, i'm seeing much better behavior 
     * with small keys when i do the xor before and after the multiply */
    hash = FNV_32_INIT;
    for (unsigned int x = 0; x < key.length (); x++) 
    {
        hash ^= key[x];
        hash *= FNV_32_PRIME;
        hash ^= key[x];
    }
    return hash / (double)UINT_MAX;
}
