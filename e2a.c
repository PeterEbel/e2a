/****************************************************************************************
 * Program:     e2a
 * Description: EBCDIC-To-ASCII Converter with packed fields (COMP-3) management
 *              for Codepage 273 (Germany)
 * Author:      Peter Ebel, peter.ebel@outlook.de
 * Date:        2017-09-27
 * Execution:   ./e2a <input ecbdic file> <output ascii file> <output metadata file> <input metadata file> <unique-number>
 *
 * Compilation: gcc -lm e2a.c -o e2a
 *
 * Change History
 * Version    By         Date        Change
 * 1.0        Ebel       2017-09-27  initial version
 * 1.1        Ebel       2017-10-06  unpack packed fields
 * 1.2        Ebel       2017-10-11  can handle CSV output
 * 1.3        Ebel       2017-10-20  refinements (safely deallocate pointers)
 * 1.4        Ebel       2018-01-29  a) unpacked binary fields must be divided by decimals
 *                                   b) INT to BIGINT in Produban metadata
 * 1.5        Ebel       2018-02-05  int trim(char *str, int iLength, TRIMBUFFER *tb) function change
 * 1.6        Ebel       2018-03-14  implement function for zoned (S) fields
 * 1.7        Ebel       2018-03-15  implement function to convert date (L) fields from DD.MM.YYYY to YYYY-MM-DD
 * 1.7.1      Ebel       2018-03-19  change the A and L types running (Code from 1.5 version, from the S type.)
 * 1.7.2      ebel       2018-03-19  fix: ConvertDateToEuro() function
 * 1.7.3      Ebel       2018-04-15  length of sFieldname in structure METADATARECORD increased from 10 to 11
 * 1.7.4      Ebel       2019-10-16  fix:     replace CR/LF in writebuffer to avoid line breaks
 *                                   fix:     to exit if an unmanaged datatype is encountered
 *                                   feature: type T treated as type A
 * 1.7.5      Ebel       2021-10-16  fix: Euro symbol in Codepage
 ****************************************************************************************/

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <math.h>
#include <time.h>
#include <ctype.h>
#include <linux/limits.h>

//main types of objects
//holds the structure metadata
typedef struct tag_metadatarecord {
  char sFieldname[11];
  char sSize[5];
  int  iInputPosition;
  int  iOutputPosition;
  int  iPrecision;
  char cDatatype;
  int  iFrom;
  int  iTo;
  int  iInputFieldLength;
  int  iOutputFieldLength;
  char sDescription[50];
  char sTranslation[50];
} METADATARECORD;

//holds metadata required by the file ingestion process
typedef struct tag_ingestionmetadata {
  char sDatabase[100];
  char sTable[20];
  int  iFieldposition;
  char sFieldname[20];
  char sDatatype[20];
  int  iLength;
  int  iPrecision;
  int  iPositionInPK;
} INGESTIONMETADATA;

//the main container
typedef struct tag_converter {
  char sDatabase[40];
  char sSchema[PATH_MAX];
  char sInputFileName[PATH_MAX];
  char sOutputFileName[PATH_MAX];
  char sIngestionMetadataFileName[PATH_MAX];
  int  iInputRecordLength;
  int  iOutputRecordLength;
  int  iNumberOfAttributes;
  int  iCurrentRecord;
  unsigned char *pReadBuffer;
  unsigned char *pWriteBuffer;
  char sUUID[36];
  FILE *fpInFile;
  FILE *fpOutFile;
  FILE *fpIngestionMetadataFile;
  METADATARECORD **Metadata;     //pointer to an array of metadata structures
} CONVERTER;

typedef struct tag_trimbuffer {
  unsigned char *pBuffer;
  int iLength;
} TRIMBUFFER;

//global buffers (easier to deallocate)
char *pDateTimeBuffer;
char *sUUID;

//forward declarations
void convert(unsigned char *, size_t);
char *GetDateTime(void);
long unpack(char *, size_t);
long unzone(char *, size_t);
int LoadMetadata(CONVERTER *);
int ExecuteCSVConversion(CONVERTER *);
int trim(char *, int, TRIMBUFFER *);
int CreateIngestionMetadataFile(CONVERTER *cv);

//Codepage 273 (for German and Austrian encodings)
static  unsigned char ebc2asc[256] =
{
  0x00, 0x01, 0x02, 0x03, 0x9C, 0x09, 0x86, 0x7F,
  0x97, 0x8D, 0x8E, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F,
  0x10, 0x11, 0x12, 0x13, 0x9D, 0x85, 0x08, 0x87,
  0x18, 0x19, 0x92, 0x8F, 0x1C, 0x1D, 0x1E, 0x1F,
  0x80, 0x81, 0x82, 0x83, 0x84, 0x0A, 0x17, 0x1B,
  0x88, 0x89, 0x8A, 0x8B, 0x8C, 0x05, 0x06, 0x07,
  0x90, 0x91, 0x16, 0x93, 0x94, 0x95, 0x96, 0x04,
  0x98, 0x99, 0x9A, 0x9B, 0x14, 0x15, 0x9E, 0x1A,
  0x20, 0xA0, 0xE2, 0x7B, 0xE0, 0xE1, 0xE3, 0xE5,
  0xE7, 0xF1, 0xC4, 0x2E, 0x3C, 0x28, 0x2B, 0x21,
  0x26, 0xE9, 0xEA, 0xEB, 0xE8, 0xED, 0xEE, 0xEF,
  0xEC, 0x7E, 0xDC, 0x24, 0x2A, 0x29, 0x3B, 0x5E,
  0x2D, 0x2F, 0xC2, 0x5B, 0xC0, 0xC1, 0xC3, 0xC5,
  0xC7, 0xD1, 0xF6, 0x2C, 0x25, 0x5F, 0x3E, 0x3F,
  0xF8, 0xC9, 0xCA, 0xCB, 0xC8, 0xCD, 0xCE, 0xCF,
  0xCC, 0x60, 0x3A, 0x23, 0xA7, 0x27, 0x3D, 0x22,
  0xD8, 0x61, 0x62, 0x63, 0x64, 0x65, 0x66, 0x67,
  0x68, 0x69, 0xAB, 0xBB, 0xF0, 0xFD, 0xFE, 0xB1,
  0xB0, 0x6A, 0x6B, 0x6C, 0x6D, 0x6E, 0x6F, 0x70,
  0x71, 0x72, 0xAA, 0xBA, 0xE6, 0xB8, 0xC6, 0x3F,
  0xB5, 0xDF, 0x73, 0x74, 0x75, 0x76, 0x77, 0x78,
  0x79, 0x7A, 0xA1, 0xBF, 0xD0, 0xDD, 0xDE, 0xAE,
  0xA2, 0xA3, 0xA5, 0xB7, 0xA9, 0x40, 0xB6, 0xBC,
  0xBD, 0xBE, 0xAC, 0x7C, 0xAF, 0xA8, 0xB4, 0xD7,
  0xE4, 0x41, 0x42, 0x43, 0x44, 0x45, 0x46, 0x47,
  0x48, 0x49, 0xAD, 0xF4, 0xA6, 0xF2, 0xF3, 0xF5,
  0xFC, 0x4A, 0x4B, 0x4C, 0x4D, 0x4E, 0x4F, 0x50,
  0x51, 0x52, 0xB9, 0xFB, 0x7D, 0xF9, 0xFA, 0xFF,
  0xD6, 0xF7, 0x53, 0x54, 0x55, 0x56, 0x57, 0x58,
  0x59, 0x5A, 0xB2, 0xD4, 0x5C, 0xD2, 0xD3, 0xD5,
  0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37,
  0x38, 0x39, 0xB3, 0xDB, 0x5D, 0xD9, 0xDA, 0x9F
};

//in-situ converter
void convert(unsigned char *pBuffer, size_t count)
{
  int i;

  for (i = 0; i < count; i++) {
    pBuffer[i] = ebc2asc[pBuffer[i]];
  }
}

//for logging
char *GetDateTime(void)
{
  time_t now = time(0);
  strftime (pDateTimeBuffer, 100, "%Y-%m-%d %H:%M:%S.000", localtime (&now));
  return pDateTimeBuffer;
}

//trim leading/tailing blanks and filter unvalid characters
int trim(char *str, int iLength, TRIMBUFFER *tb)
{

  int i, j;
  int iBegin = 0;
  int iEnd = iLength -1;
  char *pTempBuffer;

  //count leading and trailing blanks
  while (isspace((unsigned char) str[iBegin]) && (iBegin < iEnd))
    iBegin++;
  while (isspace((unsigned char) str[iEnd]) && (iEnd > iBegin))
    iEnd--;

  //copy to the temporary tempBuffer.
  if ((pTempBuffer = (char *) malloc(iEnd - iBegin + 1)) != NULL) {
    for (i = iBegin, j = 0; i <= iEnd;) {
      switch (str[i]) {
        //possibly to fix that. Break, or do something different? Meant to be skipped " characters?
        case '"':
        case '|':
          i++;
          break;
        default:
          pTempBuffer[j] = str[i];
          j++;
          i++;
          break;
      }
    }
  }
  else {
    fprintf(stdout, "%s %s [ERROR]: Could not allocate temporary trim buffer!\n", GetDateTime(), sUUID);
    exit(-1);
  }

  //indexes trimmed at the moment
  //copying proper values
  if ((tb->pBuffer = (char *) malloc(j)) != NULL) {
    memcpy(tb->pBuffer, pTempBuffer, j);
    tb->iLength = j;
    if (pTempBuffer != NULL) {
      free(pTempBuffer);
    }
  }
  else {
    fprintf(stdout, "%s %s [ERROR]: Could not allocate trim buffer!\n", GetDateTime(), sUUID);
    if (pTempBuffer != NULL) {
      free(pTempBuffer);
    }
    exit(-1);
  }
  return 0;
}

//convert zoned decimal to long
long unzone(char* pdIn, size_t length)
{

  const int DropHO = 0xFF;             // AND mask to drop HO sign bits
  const int PlusSign = 0x0F;           // Plus sign
  const int OtherNegativeSign = 0x0B;  // another minuts sign
  const int MinusSign = 0x0D;          // Minus
  const int GetLO  = 0x0F;             // Get only LO digit

  long val = 0;                        // Value to return
  int i, aByte, digit, sign;

  for (i = 0; i < length; i++) {

    aByte = pdIn[i] & DropHO;            // Get next 2 digits & drop sign bits
    val = val * 10 + (aByte & GetLO);

    if (i == length - 1) {      // last digit?
      sign = aByte >> 4;        // First get digit
      if (sign == MinusSign || sign == OtherNegativeSign) {
        val = -val;
      } else {
        if (sign != PlusSign) {
          fprintf(stdout, "%s %s [ERROR]: Invalid Sign nibble in Zoned Decimal! %d, %d\n", GetDateTime(), sUUID, sign, val);
          exit(-1);
        }
      }
    }
  }
  return val;
}

//convert packed decimal to long
long unpack(char* pdIn, size_t length)
{

  const int PlusSign = 0x0C;       // Plus sign
  const int MinusSign = 0x0D;      // Minus
  const int NoSign = 0x0F;         // Unsigned
  const int DropHO = 0xFF;         // AND mask to drop HO sign bits
  const int GetLO  = 0x0F;         // Get only LO digit

  long val = 0;                    // Value to return
  int i, aByte, digit, sign;

  for (i = 0; i < length; i++) {
    aByte = pdIn[i] & DropHO;      // Get next 2 digits & drop sign bits
    if (i == length - 1) {         // last digit?
      digit = aByte >> 4;          // First get digit
      val = val*10 + digit;
      sign = aByte & GetLO;        // now get sign
      if (sign == MinusSign) {
        val = -val;
      }
      else {
        if (sign != PlusSign && sign != NoSign) {
          fprintf(stdout, "%s %s [ERROR]: Invalid Sign nibble in Packed Decimal!\n", GetDateTime(), sUUID);
          exit(-1);
        }
      }
    }
    else {
      digit = aByte >> 4;          // HO first
      val = val*10 + digit;
      digit = aByte & GetLO;       // now LO
      val = val*10 + digit;
    }
  }
  return val;
}

int LoadMetadata(CONVERTER *cv)
{

  int i, j, k;
  char sBuffer[255];
  char *pToken;
  FILE *fpMetadataFile;

  //open the file
  if ((fpMetadataFile = fopen(cv->sSchema, "r")) != NULL) {
    i = j = 0;
    cv->iInputRecordLength = 0;
    cv->iOutputRecordLength = 0;
    cv->iNumberOfAttributes = 0;
    fprintf(stdout, "%s %s [INFO]: Reading metadata file %s\n", GetDateTime(), sUUID, cv->sSchema );
    //go through all the records
    while(!feof(fpMetadataFile)) {
      if (fgets(sBuffer, sizeof(sBuffer), fpMetadataFile) != NULL) {
        //allocate required structure pointers
        if ((cv->Metadata = (METADATARECORD **) realloc(cv->Metadata, (i+1) * sizeof (METADATARECORD *))) != NULL) {
          if((cv->Metadata[i] = (METADATARECORD *) malloc(sizeof (METADATARECORD))) != NULL) {
            j = 1;
            //tokenize each line and store data in corresponding structure fields
            pToken = strtok(sBuffer, "\t");
            while(pToken != NULL) {
              switch(j) {
                case 1:
                  strcpy(cv->Metadata[i]->sFieldname, pToken);
                  break;
                case 2:
                  strcpy(cv->Metadata[i]->sSize, pToken);
                  if (strstr(cv->Metadata[i]->sSize, ",") != NULL) {
                    for (k = 0; k < strlen(cv->Metadata[i]->sSize); k++) {
                      if (cv->Metadata[i]->sSize[k] == ',') {
                        cv->Metadata[i]->sSize[k] = '\0';
                        cv->Metadata[i]->iPrecision = atoi(&(cv->Metadata[i]->sSize[k + 1]));
                      }
                    }
                  }
                  else {
                    cv->Metadata[i]->iPrecision = 0;
                  }
                  cv->Metadata[i]->iOutputFieldLength = atoi(cv->Metadata[i]->sSize);
                  cv->Metadata[i]->iOutputPosition = (i == 0) ? 0 : cv->Metadata[i-1]->iOutputPosition + cv->Metadata[i-1]->iOutputFieldLength;
                  cv->iOutputRecordLength += cv->Metadata[i]->iOutputFieldLength;
                  break;
                case 3: cv->Metadata[i]->cDatatype=(char) pToken[0]; break;
                case 4: cv->Metadata[i]->iFrom=atoi(pToken); break;
                case 5: cv->Metadata[i]->iTo=atoi(pToken); break;
                case 6: strcpy(cv->Metadata[i]->sDescription, pToken); break;
                case 7: strcpy(cv->Metadata[i]->sTranslation, pToken); break;
                default: break;
              }
              pToken = strtok(NULL, "\t");
              j++;
            }
            cv->Metadata[i]->iInputPosition = (cv->Metadata[i]->iFrom) - 1;
            cv->Metadata[i]->iInputFieldLength = (cv->Metadata[i]->iTo - cv->Metadata[i]->iFrom) + 1;
            cv->iInputRecordLength += cv->Metadata[i]->iInputFieldLength;
          }
          else {
            fprintf(stdout, "%s %s [ERROR]: Unable to allocate metadata record!\n", GetDateTime(), sUUID);
            exit(-1);
          }
        }
        else {
          fprintf(stdout, "%s %s [ERROR]: Unable to allocate metadata table!\n", GetDateTime(), sUUID);
          exit(-1);
        }
        cv->iNumberOfAttributes++;
        i++;
      }
    }
    fclose(fpMetadataFile);
  }
  else {
    fprintf(stdout, "%s %s [ERROR]: Unable to open metadata file %s!\n", GetDateTime(), sUUID, cv->sSchema);
    exit(-1);
  }
  fprintf(stdout, "%s %s [INFO]: Metadata file %s successfully processed.\n", GetDateTime(), sUUID, cv->sSchema);
  return 0;
}

int ConvertDateToEuro(TRIMBUFFER *parm)
{

  const int SIZE_OF_THE_DATE = 10;
  const int DIGITS_IN_YEAR = 4;

  //converting char array to the string so it can be used in strtok()
  //size changed from 10 to 11, because of the NULL endings.
  char sTmpBuffer[SIZE_OF_THE_DATE + 1];
  memcpy(sTmpBuffer, parm->pBuffer, SIZE_OF_THE_DATE);
  sTmpBuffer[SIZE_OF_THE_DATE + 1] = '\0';

  //extract date parts
  char *pDayTok, *pMonTok, *pYearTokWithEnding;

  //date tokenized by dots. assumed format: 01.01.0001
  pDayTok = strtok(sTmpBuffer, ".");
  pMonTok = strtok(NULL, ".");
  pYearTokWithEnding = strtok(NULL, ".");
  
  //remove NULL ending from the char array
  char pYearTok[DIGITS_IN_YEAR];
  memcpy(pYearTok, pYearTokWithEnding, DIGITS_IN_YEAR);

  //copying to the result char array with skipped NULL char.
  char resultBuffer[SIZE_OF_THE_DATE];
  strcpy(resultBuffer, pYearTok);
  strcat(resultBuffer, "-");
  strcat(resultBuffer, pMonTok);
  strcat(resultBuffer, "-");
  strcat(resultBuffer, pDayTok);

  strcpy(parm->pBuffer, resultBuffer);
  return 0;
}

int ExecuteCSVConversion(CONVERTER *cv)
{

  //needed for debug sessions only in date conversions for type L
  //problem: some tables don't come with the exepected date format dd.mm.yyyy but with dddd-mm-yy, so ConvertDateToEuro() fails.
  //char sTest[255];

  int i, j;
  int iLastWritePosition;
  long double ldUnpacked;
  unsigned char *pUnpackBuffer;
  unsigned char *pFormatBuffer;
  TRIMBUFFER *tb;

  //we need a valid converter, so it must not be NULL
  if (cv != NULL) {
    //allocate read and write buffers based on the record lengths of source and destination files
    if ((cv->pReadBuffer = (unsigned char *) malloc(cv->iInputRecordLength)) != NULL) {
      if ((cv->pWriteBuffer = (unsigned char *) malloc(cv->iOutputRecordLength + cv->iNumberOfAttributes)) != NULL) {
      //print statistics
        fprintf(stdout, "%s %s [INFO]: Number of Attributes: %4d\n", GetDateTime(), sUUID, cv->iNumberOfAttributes);
        fprintf(stdout, "%s %s [INFO]: Input Record Length:  %4d\n", GetDateTime(), sUUID, cv->iInputRecordLength);
        fprintf(stdout, "%s %s [INFO]: Output Record Length: %4d\n", GetDateTime(), sUUID, cv->iOutputRecordLength);
        fprintf(stdout, "%s %s [INFO]: Input file:  %s\n", GetDateTime(), sUUID, cv->sInputFileName);
        fprintf(stdout, "%s %s [INFO]: Output file: %s\n", GetDateTime(), sUUID, cv->sOutputFileName);
        cv->iCurrentRecord = 1;
        //open the file to be converted
        while (!feof(cv->fpInFile)) {
          //only if there still is something to read
          if ((fread(cv->pReadBuffer, cv->iInputRecordLength, 1, cv->fpInFile)) != 0) {
            //write buffer should be zero-ed when processing a new record
            memset(cv->pWriteBuffer, 0, cv->iOutputRecordLength + cv->iNumberOfAttributes);
            iLastWritePosition = 0;
            //go through all attributes
            for (i = 0; i < cv->iNumberOfAttributes; i++) {
              switch (cv->Metadata[i]->cDatatype) {
                case 'L':
                  convert(&cv->pReadBuffer[cv->Metadata[i]->iInputPosition], cv->Metadata[i]->iInputFieldLength);
                  if ((tb = (TRIMBUFFER *) malloc(sizeof (TRIMBUFFER))) != NULL) {
                    if ((trim(&cv->pReadBuffer[cv->Metadata[i]->iInputPosition], cv->Metadata[i]->iInputFieldLength, tb)) == 0) {
                      //debug only, see comment above
                      //memcpy(sTest, tb->pBuffer, tb->iLength);
                      //sTest[tb->iLength] = '\0';
                      //printf("TrimBuffer: %s\n", sTest);
                      ConvertDateToEuro(tb);
                      memcpy(&cv->pWriteBuffer[iLastWritePosition], tb->pBuffer, tb->iLength);
                      iLastWritePosition += tb->iLength;
                    }
                    else {
                      if (tb->pBuffer != NULL) {
                        free(tb->pBuffer);
                      }
                      if (tb != NULL) {
                        free(tb);
                      }
                      fprintf(stderr, "%s %s [Error]: Trim was not successful!\n",  GetDateTime(), sUUID);
                      exit(-1);
                    }
                    if (tb->pBuffer != NULL) {
                      free(tb->pBuffer);
                    }
                    if (tb != NULL) {
                      free(tb);
                    }
                  }
                  else {
                    fprintf(stderr, "%s %s [Error]: Can't allocate trim buffer structure!\n",  GetDateTime(), sUUID);
                    exit(-1);
                  }
                  break;
                case 'A':
                case 'T':
                  convert(&cv->pReadBuffer[cv->Metadata[i]->iInputPosition], cv->Metadata[i]->iInputFieldLength);
                  if ((tb = (TRIMBUFFER *) malloc(sizeof (TRIMBUFFER))) != NULL) {
                    if ((trim(&cv->pReadBuffer[cv->Metadata[i]->iInputPosition], cv->Metadata[i]->iInputFieldLength, tb)) == 0) {
                      memcpy(&cv->pWriteBuffer[iLastWritePosition], tb->pBuffer, tb->iLength);
                      iLastWritePosition += tb->iLength;
                    }
                    else {
                      if (tb->pBuffer != NULL) {
                        free(tb->pBuffer);
                      }
                      if (tb != NULL) {
                        free(tb);
                      }
                      fprintf(stderr, "%s %s [Error]: Trim was not successful!\n",  GetDateTime(), sUUID);
                      exit(-1);
                    }
                    if (tb->pBuffer != NULL) {
                      free(tb->pBuffer);
                    }
                    if (tb != NULL) {
                      free(tb);
                    }
                  }
                  else {
                    fprintf(stderr, "%s %s [Error]: Can't allocate trim buffer structure!\n",  GetDateTime(), sUUID);
                    exit(-1);
                  }
                  break;
                case 'S':
                  ldUnpacked = 0;
                  if ((pUnpackBuffer = (unsigned char *) malloc(cv->Metadata[i]->iInputFieldLength)) != NULL) {
                    memcpy(pUnpackBuffer, &cv->pReadBuffer[cv->Metadata[i]->iInputPosition], cv->Metadata[i]->iInputFieldLength);
                    ldUnpacked = unzone(pUnpackBuffer, cv->Metadata[i]->iInputFieldLength);
                    if (pUnpackBuffer != NULL) {
                      free(pUnpackBuffer);
                    }
                    if ((pFormatBuffer = (unsigned char *) malloc(cv->Metadata[i]->iOutputFieldLength)) != NULL) {
                      //format unpacked value, divide by pow(10, number_of_decimals)
                      sprintf(pFormatBuffer, "%-*.*Lf", cv->Metadata[i]->iOutputFieldLength, cv->Metadata[i]->iPrecision, ldUnpacked / pow(10, cv->Metadata[i]->iPrecision));
                      if((tb = (TRIMBUFFER *) malloc(sizeof (TRIMBUFFER))) != NULL) {
                        if ((trim(pFormatBuffer, cv->Metadata[i]->iOutputFieldLength, tb)) == 0) {
                          memcpy(&cv->pWriteBuffer[iLastWritePosition], tb->pBuffer, tb->iLength);
                          iLastWritePosition += tb->iLength;
                          if (pFormatBuffer != NULL) {
                            free(pFormatBuffer);
                          }
                          if (tb->pBuffer != NULL) {
                            free(tb->pBuffer);
                          }
                          if (tb != NULL) {
                            free(tb);
                          }
                        }
                        else {
                          if (pFormatBuffer != NULL) {
                            free(pFormatBuffer);
                          }
                          if (tb->pBuffer != NULL) {
                            free(tb->pBuffer);
                          }
                          if (tb != NULL) {
                            free(tb);
                          }
                          fprintf(stdout, "%s %s [Error]: trim was not successful!\n", GetDateTime(), sUUID);
                          exit(-1);
                        }
                      }
                      else {
                        if (pFormatBuffer != NULL) {
                          free(pFormatBuffer);
                        }
                        fprintf(stdout, "%s %s [Error]: Can't allocate trim buffer structure!\n", GetDateTime(), sUUID);
                        exit(-1);
                      }
                    }
                    else {
                      if (pUnpackBuffer != NULL) {
                        free(pUnpackBuffer);
                      }
                      fprintf(stdout, "%s %s [Error]: Can't allocate trim buffer!\n", GetDateTime(), sUUID);
                      exit(-1);
                    }
                  }
                  else {
                    fprintf(stdout, "%s %s [Error]: Can't allocate unpack buffer!\n", GetDateTime(), sUUID);
                    exit(-1);
                  }
                  break;
                case 'P':
                  ldUnpacked = 0;
                  if ((pUnpackBuffer = (unsigned char *) malloc(cv->Metadata[i]->iInputFieldLength)) != NULL) {
                    memcpy(pUnpackBuffer, &cv->pReadBuffer[cv->Metadata[i]->iInputPosition], cv->Metadata[i]->iInputFieldLength);
                    ldUnpacked = unpack(pUnpackBuffer, cv->Metadata[i]->iInputFieldLength);
                    if (pUnpackBuffer != NULL) {
                      free(pUnpackBuffer);
                    }
                    if ((pFormatBuffer = (unsigned char *) malloc(cv->Metadata[i]->iOutputFieldLength)) != NULL) {
                      //format unpacked value, divide by pow(10, number_of_decimals)
                      sprintf(pFormatBuffer, "%-*.*Lf", cv->Metadata[i]->iOutputFieldLength, cv->Metadata[i]->iPrecision, ldUnpacked / pow(10, cv->Metadata[i]->iPrecision));
                      if ((tb = (TRIMBUFFER *) malloc(sizeof (TRIMBUFFER))) != NULL) {
                        if ((trim(pFormatBuffer, cv->Metadata[i]->iOutputFieldLength, tb)) == 0) {
                          memcpy(&cv->pWriteBuffer[iLastWritePosition], tb->pBuffer, tb->iLength);
                          iLastWritePosition += tb->iLength;
                          if (pFormatBuffer != NULL) {
                            free(pFormatBuffer);
                          }
                          if (tb->pBuffer != NULL) {
                            free(tb->pBuffer);
                          }
                          if (tb != NULL) {
                            free(tb);
                          }
                        }
                        else {
                          if (pFormatBuffer != NULL) {
                            free(pFormatBuffer);
                          }
                          if (tb->pBuffer != NULL) {
                            free(tb->pBuffer);
                          }
                          if (tb != NULL) {
                            free(tb);
                          }
                          fprintf(stdout, "%s %s [Error]: trim was not successful!\n", GetDateTime(), sUUID);
                          exit(-1);
                        }
                      }
                      else {
                        if (pFormatBuffer != NULL) {
                          free(pFormatBuffer);
                        }
                        fprintf(stdout, "%s %s [Error]: Can't allocate trim buffer structure!\n", GetDateTime(), sUUID);
                        exit(-1);
                      }
                    }
                    else {
                      if (pUnpackBuffer != NULL) {
                        free(pUnpackBuffer);
                      }
                      fprintf(stdout, "%s %s [Error]: Can't allocate trim buffer!\n", GetDateTime(), sUUID);
                      exit(-1);
                    }
                  }
                  else {
                    fprintf(stdout, "%s %s [Error]: Can't allocate unpack buffer!\n", GetDateTime(), sUUID);
                    exit(-1);                  }
                  break;
                default:
                  fprintf(stdout, "%s %s [Error]: Unmanaged Datatype!\n", GetDateTime(), sUUID);
                  exit(-1);
              } //end switch
              if (i < cv->iNumberOfAttributes - 1) {
                memset(&cv->pWriteBuffer[iLastWritePosition], '|', 1);
                iLastWritePosition += 1;
              }
              else {
                memset(&cv->pWriteBuffer[iLastWritePosition], '\n', 1);
              }
            } //end for
		        //replace CR/LF characters by some character (~) to avoid line breaks in the output
		        for (j = 0; j < iLastWritePosition; j++) {
			        if (cv->pWriteBuffer[j] == '\n' || cv->pWriteBuffer[j] == '\r') {
				        cv->pWriteBuffer[j] = '~';
			        }
		        }
            //now write buffer to output file and increase record counter
            fwrite(cv->pWriteBuffer, iLastWritePosition + 1, 1, cv->fpOutFile);
            cv->iCurrentRecord++;
          } //end fread
        } //end while
      } //malloc pWriteBuffer
      else {
        fprintf(stdout, "%s %s [ERROR]: Can't allocate write buffer.\n", GetDateTime(), sUUID);
        exit(-1);
      }
    } //malloc pReadBuffer
    else {
      fprintf(stdout, "%s %s [ERROR]: Can't allocate read buffer.\n", GetDateTime(), sUUID);
      exit(-1);
    }
  } // malloc CONVERTER
  else {
    fprintf(stdout, "%s %s [ERROR]: Converter instance is NULL.\n", GetDateTime(), sUUID);
    exit(-1);
  }
  //done
  fprintf(stdout, "%s %s [INFO]: Ready.\n", GetDateTime(), sUUID);
  return 0;
}

//metadata file required by Produban
int CreateIngestionMetadataFile(CONVERTER *cv)
{

  const char dot = '.';
  const char slash = '/';

  int i;
  char sBuffer[255];
  char *ret;
  char *pch;

  ret = strrchr(cv->sSchema, slash);
  pch = strchr(ret, dot);
  ret[pch-ret] = '\0';

  INGESTIONMETADATA im;

  if ((cv->fpIngestionMetadataFile = fopen(cv->sIngestionMetadataFileName, "w+")) != NULL) {
    fprintf(stdout, "%s %s [INFO]: Output ingestion metadata file: %s\n", GetDateTime(),  sUUID, cv->sIngestionMetadataFileName);
    for (i = 0; i < cv->iNumberOfAttributes; i++) {
      strcpy(im.sDatabase, cv->sDatabase);
      strcpy(im.sTable, &ret[1]);
      im.iFieldposition = i+1;
      strcpy(im.sFieldname, cv->Metadata[i]->sFieldname);
      switch (cv->Metadata[i]->cDatatype) {
        case 'A':
        case 'T':
          strcpy (im.sDatatype, "CHAR");
          break;
        case 'P':
        case 'S':
          if (cv->Metadata[i]->iPrecision == 0) {
            if (cv->Metadata[i]->iOutputFieldLength < 10) {
              strcpy(im.sDatatype, "INTEGER");
            }
            else {
              strcpy(im.sDatatype, "BIGINT");
            }
          }
          else {
            strcpy(im.sDatatype, "DECIMAL");
          }
          break;
        case 'L':
          strcpy(im.sDatatype, "DATE");
          break;
        default:
          break;
      }
      im.iLength = cv->Metadata[i]->iOutputFieldLength;
      im.iPrecision = cv->Metadata[i]->iPrecision;
      snprintf(sBuffer, sizeof(sBuffer), "%s|%s|%d|%s|%s|%d|%d|%d\n", im.sDatabase, im.sTable, im.iFieldposition, im.sFieldname, im.sDatatype, im.iLength, im.iPrecision, 0);
      fputs(sBuffer, cv->fpIngestionMetadataFile);
    }
  }
  fclose(cv->fpIngestionMetadataFile);
}

int main(int argc, char *argv[])
{

  int i;
  CONVERTER *cv;

  //command line has too may arguments, there is room for enhancements
  if (argc != 7) {
    fprintf(stdout, "Usage: ./e2a <input file ebcdic> <output file ascii .txt> <output file metadata .csv> <input file metadata .md> <system> <some number>\n");
    fprintf(stdout, "  - input file:      name/path of the ebdic input file\n");
    fprintf(stdout, "  - output file:     name/path of the ascii file (.txt)\n");
    fprintf(stdout, "  - metadata output: name/path of metadata output file (.csv))\n");
    fprintf(stdout, "  - metadata input:  name/path of the metaddata input file (.md)\n");
    fprintf(stdout, "  - system:          name of the system (e.g. as400)\n");
    fprintf(stdout, "  - uuid:            number used for logging purpose (generated in the wrapper)\n");
    fprintf(stdout, "Example: ./e2a /data/fivb/fivb_ebcdic /data/fivb/fivb_ascii.txt /data/fivb/fivb.csv /metadata/fivb.md as400 3b9480f8-0ada-43f0-b943-3f320d1c4f65\n");
    exit(-1);
  }
  //allocate buffer to hold datetime for logging
  pDateTimeBuffer = (char *) malloc(100);
  sUUID = (char *) malloc(36);
  strcpy(sUUID, argv[6]);
  fprintf(stdout, "%s %s [INFO]: Starting EBCDIC-ASCII File Converter v1.7.5\n", GetDateTime(),  sUUID);

  //allocate a CONVERTER structure pointer
  if ((cv = (CONVERTER *) malloc(sizeof(CONVERTER))) != NULL) {
    //copy the command line arguments into the structure
    strcpy(cv->sInputFileName, argv[1]);
    strcpy(cv->sOutputFileName, argv[2]);
    strcpy(cv->sIngestionMetadataFileName, argv[3]);
    strcpy(cv->sSchema, argv[4]);
    strcpy(cv->sDatabase, argv[5]);
    //open input and output files to read from and write to
    if ((cv->fpInFile = fopen(cv->sInputFileName, "r")) != NULL) {
      if ((cv->fpOutFile = fopen(cv->sOutputFileName, "w+")) != NULL) {
      //the three main tasks
        LoadMetadata(cv);
        CreateIngestionMetadataFile(cv);
        ExecuteCSVConversion(cv);
      }
      else {
        fprintf(stdout, "%s %s [ERROR]: Unable to open output file: %s\n", GetDateTime(), sUUID, cv->sOutputFileName);
        exit(-1);
      }
    }
    else {
      fprintf(stdout, "%s %s [ERROR]: Unable to open input file: %s\n", GetDateTime(), sUUID, &cv->sInputFileName);
      exit(-1);
    }
  }
  else {
    fprintf(stdout, "%s %s [ERROR]: Unable to  create a Converter!\n", GetDateTime(), sUUID);
    exit(-1);
  }
  //close files
  fclose(cv->fpInFile);
  fclose(cv->fpOutFile);
  //free allocated memory
  if (pDateTimeBuffer != NULL) {
    free(pDateTimeBuffer);
  }
  for (i = 0; i < cv->iNumberOfAttributes; i++) {
    if (cv->Metadata[i] != NULL) {
      free(cv->Metadata[i]);
    }
  }
  if (cv->pReadBuffer != NULL) {
    free(cv->pReadBuffer);
  }
  if (cv->pWriteBuffer != NULL) {
    free(cv->pWriteBuffer);
  }
  if (cv->Metadata != NULL) {
    free(cv->Metadata);
  }
  if (cv != NULL) {
    free(cv);
  }
  return 0;
}
