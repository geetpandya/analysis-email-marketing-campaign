/* Pig script for ETL process */

/* Loading data */

data = load 'ctordataset.csv' using PigStorage(',') as (id:int, mailid:int, mailcat:chararray, day:chararray, date:chararray, click:chararray, open:chararray, age:int, gendercode:chararray, housecode:chararray, income:chararray, asiancode:int);

/* Removing " from records */

datat = foreach data generate id, mailid, mailcat, SUBSTRING(day, 1, 4) as day, SUBSTRING(date, 1, 18) as date, click, open, age, gendercode, housecode, income, asiancode;

/* Creating new field for time of the day */

datat2 = foreach datat generate id, mailid, mailcat, day, date, click, open, age, gendercode, housecode, income, asiancode, SUBSTRING(date, 9, 14) as time;

/* Creating new filed month */

datat3_1 = foreach datat2 generate id, mailid, mailcat, day, date, click, open, age, gendercode, housecode, income, asiancode, time, SUBSTRING(date, 0, 2) as monthnumeric;

datat3 = foreach datat3_1 generate id, mailid, mailcat, day, date, click, open, age, gendercode, housecode, income, asiancode, time, (
	CASE (int)monthnumeric
	  WHEN 1 THEN 'January'
	  WHEN 2 THEN 'February'
	  WHEN 3 THEN 'March'
	  WHEN 4 THEN 'April'
	  WHEN 5 THEN 'May'
	  WHEN 6 THEN 'June'
	  WHEN 7 THEN 'July'
	  WHEN 8 THEN 'August'
	  WHEN 9 THEN 'September'
	  WHEN 10 THEN 'October'
	  WHEN 11 THEN 'November'
	  WHEN 12 THEN 'December'
	  ELSE 'ERROR'
END) as month;


/* Creating Field for salary group */

datat4 = foreach datat3 generate id, mailid, mailcat, day, date, click, open, age, gendercode, housecode, income, asiancode, time, month, (
	CASE UPPER(income)
	  WHEN 'J' THEN '<$15,000'
	  WHEN 'K' THEN '$15,000-$24,999'
	  WHEN 'L' THEN '$25,000-$34,999'
	  WHEN 'M' THEN '$35,000-$49,999'
	  WHEN 'N' THEN '$50,000-$74,999'
	  WHEN 'O' THEN '$75,000-$99,999'
	  WHEN 'P' THEN '$100,000-$119,999'
	  WHEN 'Q' THEN '$120,000-$149,999'
	  WHEN 'R' THEN '$150,000>'
	  WHEN 'U' THEN 'Unknown'
	  ELSE 'ERROR'
END) as salarygroup;

/* Creating Filed for Ethnicity */

datat5 = foreach datat4 generate id, mailid, mailcat, day, date, click, open, age, gendercode, housecode, income, asiancode, time, month, salarygroup, (
	CASE asiancode
	  WHEN 00 THEN 'Unknown'
	  WHEN 05 THEN 'Chinese'
	  WHEN 24 THEN 'Japanese'
	  WHEN 25 THEN 'Korean'
	  WHEN 47 THEN 'Vietnamese'
	  WHEN 48 THEN 'Asian'
END) as ethnicity;

/* Creating Field for household status */

datat6 = foreach datat5 generate id, mailid, mailcat, day, date, click, open, age, gendercode, housecode, income, asiancode, time, month, salarygroup, ethnicity, (
	CASE housecode
	  WHEN 'D' THEN 'Deceased'
	  WHEN 'H' THEN 'Head'
	  WHEN 'P' THEN 'Aged Parent Living Home'
	  WHEN 'U' THEN 'UNKNOWN'
	  WHEN 'W' THEN 'Spouse'
	  WHEN 'Y' THEN 'Young Adult'
END) as household;

dataf = foreach datat6 generate id, mailid, mailcat, day, date, click, open, age, gendercode, housecode, income, asiancode, time, month, salarygroup, ethnicity, household;

/* Storing Data */


store dataf into 'CTOR' using PigStorage('|');

