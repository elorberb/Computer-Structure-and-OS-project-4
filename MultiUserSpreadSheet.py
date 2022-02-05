import csv
import sys
import threading
from tkinter import *
import pandas as pd
import numpy as np
from concurrent.futures import ThreadPoolExecutor
import tksheet

class SharableSpreadSheet:
    data = None

    def __init__(self, nRows, nCols):
        self.data = self.__create_2d_array(nRows, nCols)
        self.nRows = nRows
        self.nCols = nCols
        self.__initial_semaphores(nRows, nCols)

    def get_cell(self, row, col):
        # return the string at [row,col]

        self.__rd_lock(row, col)
        self.__update_readers_count(row, col, update='add')
        if self.readers_row_count[row] == 1 or self.readers_col_count[col] == 1:
            self.__wrt_lock(row, col)
        self.__rd_release(row, col)
        cell = self.data[row][col]

        self.__rd_lock(row, col)
        self.__update_readers_count(row, col, update='reduce')
        if self.readers_row_count[row] == 0 and self.readers_col_count[col] == 0:
            self.__wrt_release(row, col)
        self.__rd_release(row, col)

        return cell

    def set_cell(self, row, col, new_str):
        self.__rd_lock(row, col)
        self.__wrt_lock(row, col)
        self.data[row][col] = new_str
        self.__rd_release(row, col)
        self.__wrt_release(row, col)
        return True

    def search_string(self,str_to_search):
        # return the first cell that contains the string [row,col]
        end_row, end_col = len(self.data), len(self.data[0])
        return self.search_in_range(0, end_col, 0, end_row, str_to_search)

    def exchange_rows(self, row1, row2):
        # exchange the content of row1 and row2
        if row1 == row2:
            return True
        self.__lock_rows(row1, row2)
        self.data[row1], self.data[row2] = self.data[row2], self.data[row1]
        self.__release_rows(row1, row2)
        return True

    def exchange_cols(self, col1, col2):
        # exchange the content of col1 and col2
        if col1 == col2:
            return True
        self.__lock_cols(col1, col2)
        for r in range(self.nRows):
            self.data[r][col1], self.data[r][col2] = self.data[r][col2], self.data[r][col1]
        self.__release_cols(col1, col2)

        return True

    def search_in_row(self, row_num, str_to_search):
        # perform search in specific row, return col number if exists.
        for c in range(len(self.data[row_num])):
            if self.get_cell(row_num, c) == str_to_search:
                return c
        # return -1 otherwise
        return -1

    def search_in_col(self, col_num, str_to_search):
        # perform search in specific col, return row number if exists.
        for r in range(len(self.data)):
            if self.get_cell(r, col_num) == str_to_search:
                return r
        # return -1 otherwise
        return -1

    def search_in_range(self, col1, col2, row1, row2, str_to_search):

        def validate_ranges(start_row, end_row, start_col, end_col, nRows, nCols):
            #end index situations
            if end_row > nRows:
                end_row = nRows
            if end_col > nCols:
                end_col = nCols
            #start index situations
            if start_row < 0:
                start_row = 0
            if start_col < 0:
                start_col = 0

            return start_row, end_row, start_col, end_col
        # perform search within specific range: [row1:row2,col1:col2]
        # includes col1,col2,row1,row2
        # return the first cell that contains the string [row,col]
        start_row, end_row = row1, row2 + 1
        start_col, end_col = col1, col2 + 1
        #validate the range
        start_row, end_row, start_col, end_col = validate_ranges(start_row, end_row, start_col, end_col, self.nRows, self.nCols)

        for r in range(start_row, end_row):
            for c in range(start_col, end_col):
                if self.get_cell(r, c) == str_to_search:
                    return [r, c]
        # return [-1,-1] if don't exists
        return [-1,-1]

    def add_row(self, row1):
        # add a row after row1:
        # add new row to end of the matrix
        self.__add_row_col_to_end(row_or_col='r')
        #moving all rows up until we reach the row1 location
        self.__move_rows(row1)
        return True

    def add_col(self, col1):
        # add a col after col1
        self.__add_row_col_to_end(row_or_col='c')
        # moving all cols up until we reach the col1 location
        self.__move_cols(col1)
        return True

    def save(self,f_name):
        file = open(f_name + ".txt", "w")
        file.writelines(f'{self.nRows},{self.nCols}')
        self.__save_index_strings(file)
        file.close()
        # save the spreadsheet to a file fileName as following:
        # nRows,nCols
        # row,col, string
        # row,col, string
        # row,col, string
        # For example 50X50 spread sheet size with only 3 cells with strings:
        # 50,50
        # 3,4,"Hi"
        # 5,10,"OOO"
        # 13,2,"EE"
        # you can decide the saved file extension.
        return True

    def load(self,f_name):
        # load the spreadsheet from fileName
        # replace the data and size of the current spreadsheet with the loaded data

        file = open(f_name + ".txt", "r") #read file
        self.__initial_new_spreadsheet(file)
        self.__update_spreadsheet(file)

    def show(self):
        # show the spreadsheet using tkinker.
        # tkinker is the default python GUI library.
        # as part of the HW you should learn how to use it.
        # there are links and simple example in the last practical lesson on model
        win = Tk()
        win.grid_columnconfigure(0, weight = 1)
        win.grid_rowconfigure(0, weight = 1)
        main_frame = Frame(win)
        main_frame.grid(row=0, column=0, sticky="nsew", padx=10, pady=10)
        entry = Entry(main_frame)
        entry.grid(row=0, column=0, sticky="ew", padx=10, pady=10)
        main_frame.grid_columnconfigure(0, weight=1)
        main_frame.grid_rowconfigure(1, weight=1)
        sheet = tksheet.Sheet(main_frame,
                      total_rows=1200,
                      total_columns=30)
        sheet.grid(row=1, column=0, sticky="nswe", padx=10, pady=10)
        sheet.set_sheet_data([[self.data[ri][cj] for cj in range(self.nCols)] for ri in range(self.nRows)])
        win.title("Etay's Spreadsheet")
        sheet.enable_bindings(bindings = "all")
        win.mainloop()

        self.data = sheet.get_sheet_data()
        return True

    #private function to use in the class

    def __create_2d_array(self, nRows, nCols):
        table = [["" for c in range(nCols)] for r in range(nRows)]
        return table

    def __initial_semaphores(self, nRows, nCols):
        #write locks
        self.wrt_row_locks = [threading.Semaphore() for r in range(nRows)]
        self.wrt_col_locks = [threading.Semaphore() for c in range(nCols)]
        #read locks
        self.rd_row_locks = [threading.Semaphore() for r in range(nRows)]
        self.rd_col_locks = [threading.Semaphore() for c in range(nCols)]
        #counters for readers in rows
        self.readers_row_count = [0 for r in range(nRows)]
        self.readers_col_count = [0 for c in range(nCols)]

    def __wrt_lock(self, row, col):
        #No reader or writer can access when writer in critical section
        #We use the order of blocking readers and then writers to prevent deadlocks
        #print(f'thread {row} , {col} start writing')

        self.wrt_row_locks[row].acquire()
        self.wrt_col_locks[col].acquire()

    def __wrt_release(self, row, col):
        #release op in the order of wrt lock func
        #print(f'thread {row} , {col} finished writing')
        self.wrt_row_locks[row].release()
        self.wrt_col_locks[col].release()

    def __update_readers_count(self, row, col, update):
        if update == 'add':
            self.readers_row_count[row] += 1
            self.readers_col_count[col] += 1
        if update == 'reduce':
            self.readers_row_count[row] -= 1
            self.readers_col_count[col] -= 1

    def __rd_lock(self, row, col):
        self.rd_row_locks[row].acquire()
        self.rd_col_locks[col].acquire()

    def __lock_rows(self, row1, row2):
        #to maintain the order of the lock we check if lock 1 is greater then lock 2
        if row1 > row2:
            self.rd_row_locks[row2].acquire()
            self.rd_row_locks[row1].acquire()
            self.wrt_row_locks[row2].acquire()
            self.wrt_row_locks[row1].acquire()
        else:
            self.rd_row_locks[row1].acquire()
            self.rd_row_locks[row2].acquire()
            self.wrt_row_locks[row1].acquire()
            self.wrt_row_locks[row2].acquire()

    def __release_rows(self, row1, row2):
        if row1 > row2:
            self.rd_row_locks[row2].release()
            self.rd_row_locks[row1].release()
            self.wrt_row_locks[row2].release()
            self.wrt_row_locks[row1].release()
        else:
            self.rd_row_locks[row1].release()
            self.rd_row_locks[row2].release()
            self.wrt_row_locks[row1].release()
            self.wrt_row_locks[row2].release()

    def __lock_cols(self, col1, col2):
        if col1 > col2:
            self.rd_col_locks[col2].acquire()
            self.rd_col_locks[col1].acquire()
            self.wrt_col_locks[col2].acquire()
            self.wrt_col_locks[col1].acquire()
        else:
            self.rd_col_locks[col1].acquire()
            self.rd_col_locks[col2].acquire()
            self.wrt_col_locks[col1].acquire()
            self.wrt_col_locks[col2].acquire()

    def __release_cols(self, col1, col2):
        if col1 > col2:
            self.rd_col_locks[col2].release()
            self.rd_col_locks[col1].release()
            self.wrt_col_locks[col2].release()
            self.wrt_col_locks[col1].release()
        else:
            self.rd_col_locks[col1].release()
            self.rd_col_locks[col2].release()
            self.wrt_col_locks[col1].release()
            self.wrt_col_locks[col2].release()

    def __rd_release(self, row, col):
        self.rd_row_locks[row].release()
        self.rd_col_locks[col].release()

    def __move_rows(self, row1):
        start = len(self.data)-2
        end = row1
        for r in range(start, end, -1):
            self.exchange_rows(r, r+1)

    def __move_cols(self, col1):
        start = len(self.data[0])-2
        end = col1
        for c in range(start, end, -1):
            self.exchange_cols(c, c+1)

    def __add_row_col_to_end(self, row_or_col):
        #if we add row
        if row_or_col == 'r':
            new_row = ["" for c in range(self.nCols)]
            self.data.append(new_row)
            self.nRows += 1 #update that a row was added
            self.rd_row_locks.append(threading.Semaphore())
            self.wrt_row_locks.append(threading.Semaphore())
            self.readers_row_count.append(0)
        #if we add col
        if row_or_col == 'c':
            for r in range(len(self.data)):
                self.data[r].append("")
            self.nCols += 1  # update that a col was added
            self.rd_col_locks.append(threading.Semaphore())
            self.wrt_col_locks.append(threading.Semaphore())
            self.readers_col_count.append(0)

    def __save_index_strings(self, file):
        for r in range(len(self.data)):
            for c in range(len(self.data[0])):
                if self.data[r][c] != "":
                    string = self.data[r][c]
                    file.writelines(f'\n{r},{c},"{string}"')

    def __initial_new_spreadsheet(self, file):
        shape = file.readline().rstrip('\n').split(',')
        self.nRows = int(shape[0])
        self.nCols = int(shape[1])
        self.data = self.__create_2d_array(self.nRows,self.nCols)
        self.__initial_semaphores(self.nRows,self.nCols)

    def __update_spreadsheet(self,file):
        lines = file.readlines() #get all lines without the length of
        for line in lines:
            values = line.rstrip('\n').split(',')
            row = int(values[0])
            col = int(values[1])
            string = values[2].replace('"', "")
            self.data[row][col] = string

    #function for testing
    def shape(self):
        return (self.nRows, self.nCols)

    def __str__(self):
         return str(self.data)

def spread_sheet_tester(nUsers, nTasks, spreadsheet):

    def get_random_arguments(strings):
        curr_task = np.random.randint(0, 10)  # total of 10 different functions to use

        # get random string
        string_pick = np.random.randint(0, len(strings))
        string = strings[string_pick]

        # get random rows and columns
        row1 = np.random.randint(0, spreadsheet.nRows)
        row2 = np.random.randint(row1, spreadsheet.nRows)
        col1 = np.random.randint(0, spreadsheet.nCols)
        col2 = np.random.randint(col1, spreadsheet.nCols)

        return curr_task, string, row1, row2, col1, col2

    # test the spreadsheet with random operations and nUsers threads.
    executor = ThreadPoolExecutor(nUsers)

    #A little bit od Eminem didn't kill nobody ;)
    strings = ['hi', 'my name is', 'hi', 'my name is', 'hi', 'my name is', 'chiki chiki', 'slim shady']
    for task in range(nTasks):
        curr_task, string, row1, row2, col1, col2 = get_random_arguments(strings)

        if curr_task == 0:
            executor.submit(spreadsheet.get_cell, row1, col1).result()
        elif curr_task == 1:
            executor.submit(spreadsheet.set_cell, row1, col1, string).result()
        elif curr_task == 2:
            executor.submit(spreadsheet.search_string, string).result()
        elif curr_task == 3:
            executor.submit(spreadsheet.exchange_rows, row1, row2).result()
        elif curr_task == 4:
            executor.submit(spreadsheet.exchange_cols, col1, col2).result()
        elif curr_task == 5:
            executor.submit(spreadsheet.search_in_row, row1, string).result()
        elif curr_task == 6:
            executor.submit(spreadsheet.search_in_col, col1, string).result()
        elif curr_task == 7:
            executor.submit(spreadsheet.search_in_range, col1, col2, row1, row2, string).result()
        elif curr_task == 8:
            executor.submit(spreadsheet.add_row, row1).result()
        elif curr_task == 9:
            executor.submit(spreadsheet.add_col, col1).result()

    return spreadsheet

def external_test(n_rows, n_cols, n_users, n_tasks):
    test_spread_sheet = SharableSpreadSheet(n_rows, n_cols)
    test_spread_sheet = spread_sheet_tester(n_users, n_tasks,test_spread_sheet)
    test_spread_sheet.show()
    test_spread_sheet.save('external_test_saved.dat')

#functions for testings
def to_pandas(data):
    df = pd.DataFrame(eval(str(data)), columns=list(np.arange(data.nCols)))
    return df

if __name__ == '__main__':
    if len(sys.argv)==5:
        external_test(n_rows=sys.argv[1],n_cols=sys.argv[2],n_users=sys.argv[3],n_tasks=sys.argv[4])
    else:
        #Internal test example (you can change it to check yourself)
        #create, test and save SharableSpreadSheet
        ss = SharableSpreadSheet(100,200)
        ss = spread_sheet_tester(50,10,ss)
        ss.show()
        ss.save('saved.dat')
        # load, test and save SharableSpreadSheet
        load_ss = SharableSpreadSheet(100,200)
        load_ss.load('saved.dat')
        load_ss = spread_sheet_tester(20,10,load_ss)
        load_ss.show()







