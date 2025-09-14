/**
 * @file transform.c
 * @brief Method which apply requested operation on file
 *
 *
 *
 * @author Simone Iori
 * @date 14.09.2025
 */

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <ctype.h>


#include "server.h"
#include "transform.h"




/**
 * @brief Calculates the checksum on the provided file
 *
 * The function works on the provided filepath, loading the
 * file from the disk and calculating the checksum.
 *
 * @param path Filepath of input file
 * @return The calculated checksum, 0 in case of errors
 */
unsigned long calc_checksum(const char *path) {

    unsigned long sum = 0;

    FILE *f = fopen(path, "rb");
    if (!f) return 0;

    unsigned char buf[4096];
    size_t r;
    while ((r = fread(buf, 1, sizeof(buf), f)) > 0) {
        for (size_t i = 0; i < r; i++) sum += buf[i];
    }

    fclose(f);
    return sum;
}

/**
 * @brief Converts the content of a file to uppercase
 *
 * @param in Filepath of input file
 * @param out Filepath of output file
 * @return Nothing
 */
void transform_upper(const char *in, const char *out) {

    FILE *fi = fopen(in, "rb");
    FILE *fo = fopen(out, "wb");

    if (!fi || !fo) { if (fi) fclose(fi); if (fo) fclose(fo); return; }

    int c;
    while ((c = fgetc(fi)) != EOF) fputc(toupper(c), fo);

    fclose(fi); fclose(fo);
}

/**
 * @brief Reverts the content of a file
 *
 * @param in Filepath of input file
 * @param out Filepath of output file
 * @return Nothing
 */
void transform_reverse_file(const char *in, const char *out) {

    FILE *fi = fopen(in, "rb");
    FILE *fo = fopen(out, "wb");

    if (!fi || !fo) { if (fi) fclose(fi); if (fo) fclose(fo); return; }

    fseek(fi, 0, SEEK_END);
    long sz = ftell(fi);
    for (long pos = sz - 1; pos >= 0; pos--) {
        fseek(fi, pos, SEEK_SET);
        int c = fgetc(fi);
        fputc(c, fo);
    }

    fclose(fi); fclose(fo);
}

/**
 * @brief Dummy operation
 *
 * It simply copies the file without changing it
 *
 * @param in Filepath of input file
 * @param out Filepath of output file
 * @return Nothing
 */
void transform_nop_file(const char *in, const char *out) {

    FILE *fi = fopen(in, "rb");
    FILE *fo = fopen(out, "wb");

    if (!fi || !fo) { if (fi) fclose(fi); if (fo) fclose(fo); return; }

    int c;
    while ((c = fgetc(fi)) != EOF) fputc(c, fo);

    fclose(fi); fclose(fo);
}

