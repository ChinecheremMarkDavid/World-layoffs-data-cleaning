-- world_layoffs Data Cleaning

/*
STEPS THAT WILL BE FOLLOWED
Remove Duplicates
Standardize the data
Null or blank values
Removing irrelevat columns */

/* Removing Duplicates

Creating another table called layoffs_prep still having the same 
data from the original tablelayoffs_preplayoffs_prep
to keep the raw data in layoffs */

create table layoffs_prep
like layoffs;

insert layoffs_prep
select * from layoffs;

/* since there is no unique id to check for duplicates, we will use the 
row number from the windows function to check for duplicates
and put the column name as row_num */

select *,
row_number() over(partition by company, location, industry, total_laid_off,
percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from layoffs_prep;

/* create a cte to be able to perform and type of analysis
on ti without affecting the original data */
  
with duplicate_cte as 
(select *,
row_number() over(partition by company, location, industry, total_laid_off,
percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from layoffs_prep
)
select * from duplicate_cte
where row_num > 1;

/* putting the new information in a new table because performing update is not possible on a cte
but we added a new column named row_num to account for the new column we added while checking for duplicates. */

CREATE TABLE `layoffs_prep2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int 
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- insert into this new table all the data we got from the cte with the newly created row-num column

insert into layoffs_prep2
select *,
row_number() over(partition by company, location, industry, total_laid_off,
percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from layoffs_prep;

/* select everything from the new table where the row number is greater than 1
greater tahn 1 means its a duplicate */

select *
from layoffs_prep2
where row_num > 1; 

-- delete the data where the row_num is greater than 1 (duplicates)

delete
from layoffs_prep2
where row_num > 1;

select * from layoffs_prep2;
  

-- Standardizing the Data
select * from layoffs_prep2;

-- trimming takes care of extra spaces 
select company, trim(company)
from layoffs_prep2;

update layoffs_prep2
set company = trim(company);

/* in the industry column, some are crypto, some are crypto currency while 
some are cryptocurrency while all are the same so we need to get all this 
data under one name. */

select *
from layoffs_prep2
where industry like 'crypto%';

update layoffs_prep2
set industry = 'crypto'
where industry like 'crypto%';

-- same thing here datas under united states and united states. should be put together

select DISTINCT country
from layoffs_prep2
order by 1;

update layoffs_prep2
set country = 'United States'
where country = 'United States.';


/* changing the date format
it was in a text formate but should be in a datetime format because its a date not text. */

select date
from layoffs_prep2;

select 	`date`, str_to_date(`date`, '%m/%d/%Y')
from layoffs_prep2;

update layoffs_prep2
set `date` = str_to_date(`date`, '%m/%d/%Y');

alter table 
layoffs_prep2
modify column `date` date;

-- Removing Null and Blank Values

-- checking for columns that either have null or blank values

select * from layoffs_prep2
where industry is null
or industry = '';

select *
from layoffs_prep2
where company = "Bally's Interactive";

update layoffs_prep2
set industry = null
where industry = '';

select t1.industry, t2.industry
from layoffs_prep2 t1
join layoffs_prep2 t2
on t1.company = t2.company
and t1.location = t2.location
where (t1.industry is null or t1. industry = '')
and t2.industry is not null;

update layoffs_prep2 t1
join layoffs_prep2 t2
on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null
and t2.industry is not null;

-- Removing irrelevant rows or columns

-- columns that have both null and blank values will be removed

select *
from layoffs_prep2
where total_laid_off is null
and percentage_laid_off is null;

delete
from layoffs_prep2
where total_laid_off is null
and percentage_laid_off is null;

select *
from layoffs_prep2;

alter table layoffs_prep2
drop column row_num;
