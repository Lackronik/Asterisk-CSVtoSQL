import csv

txt_after = open('TXTready.txt', 'w+')  # Файл с вычитаной csv базой
sql_ready = open('SQLready.txt', 'w')   # Файл с конечными запросами в SQL
# Диапазон дат которые нужно перенести
start_read = "2020-10-08"
end_read = "2020-11-10"


with open('Master.csv', 'r') as csvfile:
    rowreader = csv.reader(csvfile, delimiter=',')
    for line in rowreader:
        txt_after.write("%".join(line) + '\n')
    txt_after.seek(0)
    
    in_gap = False
    for line in txt_after:
        cur_arr = line.split('%')
        # Ищем нужный нам диапазон дат, которые нужно перенести start_read и end_read
        if end_read in line:
            in_gap = False
            break

        if start_read in line:
            in_gap = True
        # Работа в диапазоне заданых дат
        if in_gap:
            if str(cur_arr[10]) == "":
                cur_arr[10] = cur_arr[9]
            time = str(cur_arr[9]).replace(" ", "-")
            perifiral_string = f"VALUES ('{cur_arr[9]}', '{cur_arr[4]}', '{cur_arr[1]}', '{cur_arr[2]}', '{cur_arr[3]}', '{cur_arr[5]}', '{cur_arr[6]}', '{cur_arr[7]}', '{cur_arr[8]}', {cur_arr[12]}, {cur_arr[13]}, '{cur_arr[9]}', '{cur_arr[10]}', '{cur_arr[11]}', '{cur_arr[14]}', 3, '{cur_arr[16]}-{time}-{cur_arr[1]}-s', '{cur_arr[16]}')"
        
            final_string = f"INSERT INTO cdr (calldate, clid, src, dst, dcontext, channel, dstchannel, lastapp, lastdata, duration, billsec, start, answer, end, disposition, amaflags, filename, uniqueid) {perifiral_string};"
            sql_ready.write(final_string + '\n')
