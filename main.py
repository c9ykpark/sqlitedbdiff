# coding=utf-8

import sqlite3
import csv

FILE1=""
FILE2=""
TABLE_NAME1="Files"
TABLE_NAME2="Files"
COL="fileID"
COL_IDX=0

RESULT_FILE_PATH = "./backup_list_diff.csv"

# 간단한 알고리즘 설명
# Table A, Table B를 비교하고, 추가된 행을 따로 기록한다.
# Table A에는 있으나, Table B에는 없는 경우 => Table A에 있는 파일 정보를 기록하고 A는 다음 행으로. B는 그대로.
# Table B에는 있으나, Table A에는 없는 경우 => Table B에 있는 파일 정보를 기록하고 B는 다음 행으로. A는 그대로.
# Table A, Table B 모두 있는 경우 => A, B 모두 다음행으로.
# Table A, Table B의 내용은 이미 정렬되어 있다는 가정하에 수행한다.

if __name__ == '__main__':
    con1 = sqlite3.connect(FILE1)
    con2 = sqlite3.connect(FILE2)

    cur1 = con1.cursor()
    cur2 = con2.cursor()

    query1 = "SELECT * FROM " + TABLE_NAME1 + " ORDER BY " + COL
    query2 = "SELECT * FROM " + TABLE_NAME2 + " ORDER BY " + COL

    cur1.execute(query1)
    cur2.execute(query2)

    rows1 = cur1.fetchone()
    rows2 = cur2.fetchone()

    result_fd = open(RESULT_FILE_PATH, 'w')
    writer = csv.writer(result_fd)

    while (rows1 != None) and (rows2 != None):
        #print("[LOG] Comparing "+ rows1[0] + " and " + rows2[0])
        if rows1[COL_IDX] == rows2[COL_IDX]:
            rows1 = cur1.fetchone()
            rows2 = cur2.fetchone()
        else:
            # Table A 보다 Table B가 크다 = 사전순위 상 Table A가 먼저다 = Table B행에 없고 Table A만 있다.
            if rows1[COL_IDX] < rows2[COL_IDX]:
                #print("[1] "+ rows1[0])
                writer.writerow([1, rows1[0], rows1[1], rows1[2], rows1[3]])
                rows1 = cur1.fetchone()
            else:
                #print("[2] "+rows2[0])
                writer.writerow([2, rows2[0], rows2[1], rows2[2], rows2[3]])
                rows2 = cur2.fetchone()

    # Table A나 Table B 중 하나가 끝나면 잔여 행을 채워넣어 마무리한다.
    while rows1 != None:
        #print("[1] "+rows1[0])
        writer.writerow([1, rows1[0], rows1[1], rows1[2], rows1[3]])
        rows1 = cur1.fetchone()

    while rows2 != None:
        #print("[2] "+rows2[0])
        writer.writerow([2, rows2[0], rows2[1], rows2[2], rows2[3]])
        rows2 = cur2.fetchone()

    result_fd.close()
    con2.close()
    con1.close()

