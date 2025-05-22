-- Load employee data
EMPLOYEES = LOAD 'employees.csv' USING PigStorage(',')  
            AS (id:INT, name:CHARARRAY, age:INT, department:CHARARRAY, salary:DOUBLE);

-- Load department data
DEPARTMENTS = LOAD 'departments.csv' USING PigStorage(',')  
              AS (dept_id:CHARARRAY, dept_name:CHARARRAY);

-- Filter employees with salary > 50000
FILTERED_EMPLOYEES = FILTER EMPLOYEES BY salary > 50000;

-- Group by department
GROUPED_EMPLOYEES = GROUP FILTERED_EMPLOYEES BY department;

-- Join with department data
JOINED_DATA = JOIN FILTERED_EMPLOYEES BY department, DEPARTMENTS BY dept_id;

-- Select required columns
PROJECTED_DATA = FOREACH JOINED_DATA GENERATE id, name, age, dept_name, salary;

-- Sort by salary descending
SORTED_DATA = ORDER PROJECTED_DATA BY salary DESC;

-- Store output
STORE SORTED_DATA INTO 'output' USING PigStorage(',');
