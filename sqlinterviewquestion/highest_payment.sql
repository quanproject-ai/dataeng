
-- create
CREATE TABLE EMPLOYEE (
  employee_id INTEGER PRIMARY KEY,
  employee_name TEXT NOT NULL,
  dept TEXT NOT NULL,
  salary decimal(10,2) NOT NULL
);

-- insert
INSERT INTO EMPLOYEE (employee_id, employee_name, dept, salary) VALUES
(1, 'Alice Smith', 'Sales', 70000.00),
(2, 'Bob Johnson', 'IT',80000.00),
(3, 'Charlie Brown', 'HR',75000.00),
(4, 'David Wilson', 'IT',60000.00),
(5, 'Eva Green', 'HR',90000.00),
(6, 'Frank Miller', 'Engineering',55000.00),
(7, 'Grace Lee', 'Engineering',65000.00),
(8, 'Henry Taylor', 'Engineering',72000.00),
(9, 'Ivy Anderson', 'Admin',78000.00),
(10, 'Jack Thompson', 'Admin',50000.00),
(11, 'Jack Kayton', 'Sales',50000.00);
-- fetch 
-- person with highest salary of each department

-- SELECT 
-- iq.employee_name,
-- iq.dept,
-- iq.salary
-- from 
-- (SELECT
--     employee_name,
--     dept,
--     salary,
--     dense_rank() over(partition by dept order by salary desc) as ranking
--     FROM
--     EMPLOYEE) as iq
-- where iq.ranking = 1


WITH CTE AS (SELECT
    employee_name,
    dept,
    salary,
    dense_rank() over(partition by dept order by salary desc) as ranking
    FROM
    EMPLOYEE)
SELECT 
employee_name,
dept,
salary
FROM CTE where ranking =1