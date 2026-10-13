#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
import random

def writeToCsvFile(string,afile,pos):
    for c in string:
        afile.write("<p" + str(pos) + ">,<" + c + ">\n")
        pos+=1
    return pos

if len(sys.argv) != 3:
    print("Usage: make-test-strings.py <length> <name>")
    print(" length: the approximate length of the strings, a number")
    print(" name: the string to be used in generated file names, e.g., lcs100 if the number was 100")
    print("The script generates several files to be loaded in Datalog.")
    sys.exit()

length = int(sys.argv[1])
name = sys.argv[2]
file_doca = name + "-doca.csv"
file_docb = name + "-docb.csv"
file_succ = name + "-succ.csv"
file_letters = name + "-letters.csv"

letters = 'abcdefghijklmnopqrstuvwxyz'

doca_file = open(file_doca, 'w')
docb_file = open(file_docb, 'w')


posa = 1
posb = 1

for i in range(0, length):
    stringa = random.choice(letters)
    stringb = stringa
    # Change some content:
    if random.randint(1,100) <= 25:
        match random.randint(1,5):
            case 1:
                stringb = random.choice(letters)
            case 2:
                stringa = ''
            case 3:
                stringb = ''
            case 4:
                for j in range(0,random.randint(1,5)):
                    stringa = stringa + random.choice(letters)
            case 5:
                for j in range(0,random.randint(1,5)):
                    stringb = stringb + random.choice(letters)

    posa = writeToCsvFile(stringa,doca_file,posa)
    posb = writeToCsvFile(stringb,docb_file,posb)

doca_file.write("<p" + str(posa) + ">,<ENDOFA>\n")
docb_file.write("<p" + str(posb) + ">,<ENDOFB>\n")
posa += 1
posb += 1
doca_file.close()
docb_file.close()


letters_file = open(file_letters, 'w')
for i in range(1,len(letters)):
    letters_file.write("<" +letters[i-1] + ">,<" + letters[i] + ">\n")
letters_file.write("<" +letters[i] + ">,<ENDOFA>\n")
letters_file.write("<ENDOFA>,<ENDOFB>\n")
letters_file.close()

maxpos = max(posa,posb)
succ_file = open(file_succ, 'w')
for i in range(1,maxpos):
    succ_file.write("<p" + str(i-1) + ">,<" + "p" + str(i) + ">\n")
succ_file.close()



