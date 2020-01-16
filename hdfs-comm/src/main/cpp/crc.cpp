#include <com_bushpath_anamnesis_checksum_NativeChecksumCRC32.h>

#include <netinet/in.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <iostream>
#include <fstream>

#include "crc32_zlib_polynomial_tables.h"
#include "crc32c_tables.h"

using namespace std;

uint32_t crc32c_sb8(uint32_t crc, const uint8_t *buf, size_t length);
uint32_t crc32_zlib_sb8(uint32_t crc, const uint8_t *buf, size_t length);

JNIEXPORT void JNICALL
    Java_com_bushpath_anamnesis_checksum_NativeChecksumCRC32_nativeBulkCompute(
        JNIEnv *env, jobject obj, jbyteArray j_buffer, jint offset, jint length,
        jbyteArray j_cbuffer, jint coffset, jint clength, jint bytes_per_checksum) {

    // convert arguments into usable types
    jbyte* buffer = env->GetByteArrayElements(j_buffer, NULL);
    uint8_t *buffer_index = (uint8_t*)buffer;
    buffer_index += offset;

    jbyte* cbuffer = env->GetByteArrayElements(j_cbuffer, NULL);
    uint8_t *cbuffer_index = (uint8_t*)cbuffer;
    cbuffer_index += coffset;

    // iterate over buffer elements and compute crc values
    int32_t remaining_bytes = length - offset;
    while (remaining_bytes > 0) {
        uint32_t crc = 0xFFFFFFFF;
        uint32_t checksum_size =
            (remaining_bytes < bytes_per_checksum) ? remaining_bytes : bytes_per_checksum;
        crc = crc32c_sb8(crc, buffer_index, checksum_size);

        // put checksum in cbuffer
        (uint32_t&)*cbuffer_index = ntohl(~crc);
        cbuffer_index += 4;

        buffer_index += bytes_per_checksum;
        remaining_bytes -= bytes_per_checksum;
    }
 
    // release arguments
    env->ReleaseByteArrayElements(j_buffer, buffer, 0);
    env->ReleaseByteArrayElements(j_cbuffer, cbuffer, 0);

    return;
}

JNIEXPORT jint JNICALL 
    Java_com_bushpath_anamnesis_checksum_NativeChecksumCRC32_nativeCompute(
        JNIEnv *env, jobject obj, jbyteArray j_buffer,
        jint offset, jint length) {

    // convert arguments into usable types
    jbyte* buffer = env->GetByteArrayElements(j_buffer, NULL);
    uint8_t *buffer_index = (uint8_t *)buffer;
    buffer_index += offset;

    // compute hadoop style crc
    uint32_t crc = 0xFFFFFFFF;
    crc = crc32c_sb8(crc, buffer_index, length);

    // release arguments
    env->ReleaseByteArrayElements(j_buffer, buffer, 0);

    return ~crc;
}

/**       
 * Computes the CRC32c checksum for the specified buffer using the slicing by 8
 * algorithm over 64 bit quantities.
 */         
uint32_t crc32c_sb8(uint32_t crc, const uint8_t *buf, size_t length) {
  uint32_t running_length = ((length)/8)*8;
  uint32_t end_bytes = length - running_length;
  int li;
  for (li=0; li < running_length/8; li++) {
    uint32_t term1;
    uint32_t term2;
    crc ^= *(uint32_t *)buf;
    buf += 4;
    term1 = CRC32C_T8_7[crc & 0x000000FF] ^
        CRC32C_T8_6[(crc >> 8) & 0x000000FF];
    term2 = crc >> 16;
    crc = term1 ^
        CRC32C_T8_5[term2 & 0x000000FF] ^
        CRC32C_T8_4[(term2 >> 8) & 0x000000FF];
    term1 = CRC32C_T8_3[(*(uint32_t *)buf) & 0x000000FF] ^
        CRC32C_T8_2[((*(uint32_t *)buf) >> 8) & 0x000000FF];
              
    term2 = (*(uint32_t *)buf) >> 16;
    crc =  crc ^
        term1 ^
        CRC32C_T8_1[term2  & 0x000000FF] ^
        CRC32C_T8_0[(term2 >> 8) & 0x000000FF];
    buf += 4;
  }
  for (li=0; li < end_bytes; li++) {
    crc = CRC32C_T8_0[(crc ^ *buf++) & 0x000000FF] ^ (crc >> 8);
  }
  return crc;
}

/**
 * Update a CRC using the "zlib" polynomial -- what Hadoop calls CHECKSUM_CRC32
 * using slicing-by-8
 */
uint32_t crc32_zlib_sb8(
    uint32_t crc, const uint8_t *buf, size_t length) {
  uint32_t running_length = ((length)/8)*8;
  uint32_t end_bytes = length - running_length;
  int li;
  for (li=0; li < running_length/8; li++) {
    uint32_t term1;
    uint32_t term2;
    crc ^= *(uint32_t *)buf;
    buf += 4;
    term1 = CRC32_T8_7[crc & 0x000000FF] ^
        CRC32_T8_6[(crc >> 8) & 0x000000FF];
    term2 = crc >> 16;
    crc = term1 ^
        CRC32_T8_5[term2 & 0x000000FF] ^
        CRC32_T8_4[(term2 >> 8) & 0x000000FF];
    term1 = CRC32_T8_3[(*(uint32_t *)buf) & 0x000000FF] ^
        CRC32_T8_2[((*(uint32_t *)buf) >> 8) & 0x000000FF];

    term2 = (*(uint32_t *)buf) >> 16;
    crc =  crc ^
        term1 ^
        CRC32_T8_1[term2  & 0x000000FF] ^
        CRC32_T8_0[(term2 >> 8) & 0x000000FF];
    buf += 4;
  }
  for (li=0; li < end_bytes; li++) {
    crc = CRC32_T8_0[(crc ^ *buf++) & 0x000000FF] ^ (crc >> 8);
  }
  return crc;
}
