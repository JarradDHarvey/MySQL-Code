-- SQL PROJECT DATA Cleaning

-- created new Schemas and added Layoff data from AlexTheAnalyst from gethub.com

-- look at the table to see the data 
SELECT*
FROM layoffs;

-- first thing to do was create a staging table. This is the one that I'm working to clean the data. duplicate table with the raw data in case something happens

CREATE TABLE layoffs_staging
LIKE layoffs;


SELECT *
FROM layoffs_staging;

INSERT layoffs_staging
SELECT *
FROM layoffs;

-- 1. check for duplicates and remove any
-- 2. standardize data and fix errors
-- 3. Look at null values and see what 
-- 4. remove any columns and rows that are not necessary - few way

-- 1. Remove Duplicates

# First let's check for duplicates


SELECT *,
ROW_NUMBER () OVER( 
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM layoffs_staging;

-- these are the ones we want to delete where the row number is > 1 or 2or greater essentially
 
SELECT *
FROM duplicate_cte
WHERE row_num >1;

-- used CTE instead of Subquerie 

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER () OVER( 
PARTITION BY company, location,
industry, total_laid_off, percentage_laid_off, `date`, 
stage ,country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

-- looked at Casper to confirm

SELECT *
FROM layoffs_staging
WHERE company = 'Casper';


WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER () OVER( 
PARTITION BY company, location,
industry, total_laid_off, percentage_laid_off, `date`, 
stage ,country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
DELETE
FROM duplicate_cte
WHERE row_num > 1;

-- one solution, which I think is a good one. Is to create a new column and add those row numbers in. Then delete where row numbers are over 2, then delete that column
-- so let's do it!!

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
 
SELECT *
FROM layoffs_staging2
WHERE row_num > 1;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER( 
PARTITION BY company, location,
industry, total_laid_off, percentage_laid_off, `date`, 
stage ,country, funds_raised_millions) AS row_num
FROM layoffs_staging;



DELETE
FROM layoffs_staging2
WHERE row_num > 1;

SELECT *
FROM layoffs_staging2;


-- 2. Standardizing Data

SELECT company,TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

-- Looked at industry it looks like we have some null and empty rows, let's take a look at these

SELECT DISTINCT industry
FROM layoffs_staging2
;

-- I also noticed the Crypto has multiple different variations. I standardize that -  all to Crypto

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- Also looked a Country
SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

-- I have some "United States" and some "United States." with a period at the end. I standardize this.
UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

--  fix the date columns:
SELECT `date`
FROM layoffs_staging2;

-- Used str to date to update this field
UPDATE layoffs_staging2
Set `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- Convert the data type properly
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- 3. Look at Null Values

-- the null values in total_laid_off, percentage_laid_off, and funds_raised_millions all look normal. I don't think I want to change that
-- I like having them null because it makes it easier for calculations during the EDA phase

-- so there isn't anything I want to change with the null values
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Set the blanks to nulls since those are typically easier to work with

UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- Checked those are all null
SELECT *
FROM layoffs_staging2
WHERE industry is NULL
OR industry = '';

-- Looked for other errors
SELECT *
FROM layoffs_staging2
WHERE company LIKE 'Bally%';


SELECT t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

-- Populate nulls if possible
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

SELECT *
FROM layoffs_staging2;

-- 4. remove any columns and row
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Delete Useless data 
DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;


SELECT *
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

