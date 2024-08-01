-- Create the user_logins table
CREATE TABLE user_logins (
    user_id INT,
    login_date DATE
);

-- Insert sample data
INSERT INTO user_logins (user_id, login_date) VALUES
(1, '2024-07-20'), -- Saturday
(1, '2024-07-21'), -- Sunday
(1, '2024-07-22'), -- Monday
(1, '2024-07-23'), -- Tuesday
(1, '2024-07-24'), -- Wednesday
(2, '2024-07-20'), -- Saturday
(2, '2024-07-21'), -- Sunday
(2, '2024-07-22'), -- Monday
(2, '2024-07-24'), -- Wednesday
(3, '2024-07-22'), -- Monday
(3, '2024-07-23'), -- Tuesday
(3, '2024-07-24'), -- Wednesday
(3, '2024-07-25'), -- Thursday
(4, '2024-07-18'), -- Thursday
(4, '2024-07-19'), -- Friday
(4, '2024-07-21'), -- Sunday (missing 7-20 so does not count)
(5, '2024-07-22'), -- Monday
(5, '2024-07-23'), -- Tuesday
(5, '2024-07-24'), -- Wednesday
(6, '2024-07-20'), -- Saturday
(6, '2024-07-22'); -- Monday (Missing 21 for no consecutive days)

WITH cont_login as (
select user_id,
login_date,
row_number() over(partition by user_id order by login_date) as rn,
DATE_SUB(login_date, INTERVAL 1 day) as yesterday
from user_logins
)

/* solution will require to left join alot if the request increases to more than 2 consecutive days*/
select  cl3.user_id
from cont_login cl1
left join  cont_login  cl2 
on cl1.user_id = cl2.user_id and cl1.login_date = cl2.yesterday
left join cont_login cl3
on cl2.user_id = cl3.user_id and cl2.login_date = cl3.yesterday
where cl3.user_id  is not null 
group by user_id

/*this silution only rquire to modify the integer in the datediff and rn math*/
select  distinct
cl2.user_id
from cont_login cl1 
left join cont_login cl2 on 
cl1.user_id = cl2.user_id
and datediff(cl2.login_date,cl1.login_date)=2
and cl2.rn - cl1.rn =2
WHERE cl2.user_id is not null



