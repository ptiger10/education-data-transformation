
# coding: utf-8

# In[1]:

import pandas as pd
import numpy as np
# import Native_Drive_API_Helper
import sys


# In[ ]:

report_name = sys.argv[1]
report_filters = sys.argv[2]


# In[2]:

def main():
     make_standard_report(report_name, districts=getattr(get_filters(), report_filters), printer='csv')


# # Helper Functions

# In[3]:

def filter_data(df, columns=None, counties=None, districts=None, year=None):
    """
    input: dataframe to filter; list of key codes (columns) & list of key counties (rows)
    output: dataframe filtered based on code/county inclusion
    
    params:
        - df (df): dataframe to filter
        - key_columns (list) *optional: list of codes to exclusively include as columns
        - key_counties (list) *optional: list of counties to exclusively include as rows
        """
    if columns:
        new_columns = []
        for item in columns:
            if item in list(df.data.columns):
                new_columns.append(item)
        df.filtered_data = df.data[new_columns]
    df = df.district_reference.merge(df.filtered_data)
    if counties:
        df = df[df['COUNTY'].isin(counties)]
    if districts:
        df = df[df['DISTRICT'].isin(districts)]
    if year:
        df = df[df['YEAR'].isin(year)]
    return df

def transform_columns_inplace(df, columns, func):
    for column in columns:
        df[column] = df[column].apply(func)
    return df


# In[4]:

class Data():
    def __init__(self, source, years, datatype, description):
        if years:
            self.data = self.initialize_dataframe(source, years)
        else:
            self.data = pd.read_csv(source, header=0, low_memory=False)
        self.datatype = datatype
        self.description = description
        self.district_reference = pd.read_csv('DREF.dat', sep=',', header=0, 
                                              usecols=['DISTRICT', 'DISTNAME', 'COUNTY','CNTYNAME','REGION'], 
                                              low_memory=False)
        self.parse_codes()
    
    def initialize_dataframe(self, file_source, years):
        """
        input: file source containing raw dataframes for each year; list of years
        output: concatenated dataframe containing data from all years, including year column

        params:
            - file_source (str): naming convention used in the base file source
            - years (list): list of year strings in 'YYYY' format
        """

        final_df = pd.DataFrame([])
        for year in years:
            df = pd.read_csv(file_source + '_' + year + '.dat', sep=',', header=0, low_memory=False)
            df['YEAR'] = year
            final_df = pd.concat([final_df, df])
        return final_df
    
    def parse_codes(self):
        if self.datatype == 'dstud':
            self.code_data = pd.read_csv('dstud_codes.csv', sep=',', header=None)
            self.codes = self.code_data[0].str.split('--', 1).str[0].str.strip()
            self.code_stubs = [elem[:5] + elem[7] if elem[5:6].isdigit() and 'DPETG' not in elem else elem for elem in self.codes]
            self.stub_descriptions = self.code_data[0].str.split('--', 1).str[1].str.split(': ',1).str[1].str.strip()
            self.stub_dictionary = dict(zip(self.code_stubs, self.stub_descriptions))
            
    def transform_column_years_in_codes(self, base_list, years):
        """
        inputs: code string with an embedded year at a specific position; list of years
        output: new code string with the year altered
        notes: the location of the alteration depends on the length of each code string

        params:
            - codes (df): table with a column entitled 'DISTRICT' containing all the codes for transformation
            - years (list): list of year strings in 'YY' format to be altered within the base list; can be empty
        """
        master_list = []
        master_list.extend(base_list)
        for year in years:
            if len(base_list[0])==12:
                new_list = [elem[0:9]+year+elem[11] if elem[9:10].isdigit() else elem for elem in base_list]

            if len(base_list[0])==8:
                new_list = [elem[0:5]+year+elem[7] if elem[5:6].isdigit() and 'DPETG' not in elem                             else elem for elem in base_list]

            master_list.extend(new_list)

        return master_list


# In[16]:

class Filters():
    def __init__(self):
        self.key_columns = ['DISTRICT', 'YEAR']
        self.staar_columns_2016 = self.key_columns + ['DA00AR01S16R', 'DB00AR01S16R', 'DH00AR01S16R', 'DL00AR01S16R', 'DE00AR01S16R', 'DS00AR01S16R',
                              'DA00AW01S16R', 'DB00AW01S16R', 'DH00AW01S16R', 'DL00AW01S16R', 'DE00AW01S16R', 'DS00AW01S16R',
                              'DA00AM01S16R', 'DB00AM01S16R', 'DH00AM01S16R', 'DL00AM01S16R', 'DE00AM01S16R', 'DS00AM01S16R']
        
        self.enrollment_columns = self.key_columns + [
            'DPETECOP', 'DPETBLAP', 'DPETHISP', 'DPETBILP', 'DPETLEPP', 'DPETSPEP',
            'DPETALLC', 'DPETBILC', 'DPETBLAC', 'DPETHISC', 'DPETLEPC', 'DPETSPEC','DPETECOC']
        
        self.grade_columns = self.get_grade_columns()
        self.discipline_columns = ['DISTRICT', 'STUDENT GROUP', 'OSS STUDENTS', 'ISS STUDENTS', 'NUMBER OF STUDENTS']
        
        self.dallas_counties = {57: 'DALLAS', 220: 'TARRANT'}
        self.houston_districts = {101828: 'Houston Gateway', 101912:'Houston ISD'}
        self.basis_districts = {15834: 'Basis Texas',
                              15907: 'San Antonio ISD',
                              15901: 'Alamo Heights ISD',
                              15915: 'Northside ISD'}
        
        self.new_houston_districts = {101802:'Ser-Ninos Charter School',
                                      101806:'Raul Yzaguirre School for Success',
                                      101814:'The Varnett Public School',
                                      101828:'Houston Gateway Academy Inc',
                                      101853:'Promise Community School',
                                      101912:'Houston ISD'}



    def get_grade_columns(self):
        grade_column_roots = ['DPETG']*12
        nums = range(1,13)
        nums = [str(item).zfill(2) if item<10 else str(item) for item in nums]
        grade_only_columns = [x + y + 'C' for x, y in zip(grade_column_roots, nums)]
        grade_only_columns.insert(0, 'DPETGKNC') 
        grade_only_columns.insert(0, 'DPETGPKC')
        grade_columns = self.key_columns + grade_only_columns
        return grade_columns


# In[20]:

def get_filters():
    return Filters()
def get_enrollment_data(): 
    years = ['2014', '2015', '2016']
    return Data('DISTPROF', years , 'dstud', 'Student Enrollment Data')

def get_performance_data():
    years = ['2014', '2015', '2016']
    return Data('DISTSTAAR2', years, 'staar', 'Student Performance Data')

def get_discipline_data(columns, counties, districts):
    d = Data('discipline_region4_2016.csv', None, 'disc', 'Student Disciplinary Data')
    df = filter_data(d, columns=columns,
                         counties=counties, districts=districts)
    
    df = df.pivot_table(index=['DISTRICT','DISTNAME', 'COUNTY'], values=['OSS STUDENTS', 'ISS STUDENTS', 'NUMBER OF STUDENTS'], 
              columns='STUDENT GROUP')
    
    student_groups = ['ALL STUDENTS', 'BLACK OR AFRICAN AMERICAN', 'HISPANIC/LATINO', 'SPECIAL ED.', 'ECON. DIS.']
    
    df = df.stack(0)[student_groups]
    return df


# In[2]:

def make_standard_report(name, counties=None, districts=None, printer=None):
    f = get_filters()
    enrollment = filter_data(get_enrollment_data(), 
                     columns=f.enrollment_columns, counties=counties, districts=districts)
    
    grades = filter_data(get_enrollment_data(), 
                     columns=f.get_grade_columns(), counties=counties, districts=districts)
    
    performance = filter_data(get_performance_data(), 
                     columns=f.staar_columns_2016, counties=counties, districts=districts, year=['2016'])
    
    discipline = get_discipline_data(columns= f.discipline_columns, counties=counties, districts=districts).reset_index()
    
    if printer=='csv':
        enrollment.to_csv(name+' Enrollment.csv')
        grades.to_csv(name+' Grades.csv')
        performance.to_csv(name+' Performance.csv')
        discipline.to_csv(name+ ' Discipline.csv')
    elif printer=='gsheet':
        Native_Drive_API_Helper.write_to_sheets('1i7R-ER7zZx-W1sxkCe-y6ueZTbAz1QFh7LWUGOuWR74', 'enrollment', enrollment)
        Native_Drive_API_Helper.write_to_sheets('1i7R-ER7zZx-W1sxkCe-y6ueZTbAz1QFh7LWUGOuWR74', 'grades', grades)
        Native_Drive_API_Helper.write_to_sheets('1i7R-ER7zZx-W1sxkCe-y6ueZTbAz1QFh7LWUGOuWR74', 'performance', performance)
        Native_Drive_API_Helper.write_to_sheets('1i7R-ER7zZx-W1sxkCe-y6ueZTbAz1QFh7LWUGOuWR74', 'discipline', discipline)
    else:
        return enrollment, grades, performance, discipline
    


# In[9]:

# make_standard_report('Basis',districts=get_filters().basis_districts)


# In[24]:

# make_standard_report('New Houston',districts=get_filters().new_houston_districts, print='gsheet')


# In[ ]:




# In[ ]:

if __name__ == '__main__':
    main()


# In[6]:

# !jupyter nbconvert --to=python 20170410_TXSchoolReport.ipynb --output=TX_report_generator.py


# In[ ]:



