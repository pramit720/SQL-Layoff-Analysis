# SQL-Layoff-Analysis Project


**Project Overview**

This project involves the cleaning, transformation, and analysis of a dataset containing information on company layoffs. The main objective is to prepare the data for analysis by removing duplicates, standardizing fields, handling missing values, and then deriving insights through SQL queries.

**Objective**

-	Data Cleaning: Remove duplicate records, standardize text fields, and handle null values to ensure data integrity.
-	Data Transformation: Convert date formats and apply necessary data type conversions.
-	Data Analysis: Perform trend analysis on layoffs, including monthly and yearly aggregations, and identify top companies based on layoffs.


**Tools Used**

-	SQL: Complex queries, CTEs (Common Table Expressions), window functions, string manipulation, and date handling.

**Key SQL Queries**:
**- Duplicate Removal:**
WITH duplicate_cte AS (
  SELECT *,
  ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off) as row_num
  FROM layoffs_staging2
)
DELETE FROM layoffs_staging2
WHERE row_num > 1;


**- Standardizing Company Names:**
UPDATE layoffs_staging2
SET company = TRIM(company);

**- Handling Null Values:**
UPDATE layoffs_staging2
SET industry = t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
WHERE t1.industry IS NULL;

**- Trend Analysis:**
SELECT YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY 2 DESC;
    
