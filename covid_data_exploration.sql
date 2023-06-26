
/*
andCovid 19 Data Exploration
Skills used: Joins, CTE's, Temp Tables, Windows Functions, Aggregate Functions, Creating Views, Converting Data Types
*/

 --this dataset contain information about Covid deaths: 
select 
	iso_code, continent, location, date, total_cases, total_deaths
from portfolio_project..covid_death_test_hospitalization
where continent is not null and total_cases is not null;

-- 1. Total cases vs. Total Deaths: death rates
/* 1.1. Generate death rates: death rates varies from day to day and country to country. 
   Overall trend is death rates gradually decreased over time*/
select iso_code, continent, location, date, total_cases, total_deaths, (cast(total_deaths as decimal)/total_cases)*100 as death_rate
from portfolio_project..covid_death_test_hospitalization
where continent is not null and total_cases is not null and total_deaths is not null 
order by iso_code, date
 

/*	1.2. When sorting the data by death_rate, we find that some countries have total_deaths>total_cases.
	which creates unsually high death_rate (>100%) and logically does not make sense. 
	
	Therefore, the condition that total_cases>total_deaths is added to the where clause 
	to account for the irregular data.  
	
	This could be attributable to some data entry errors but further 
	investigation is needed to confirm this. 
*/ 
select iso_code, continent, location, date, total_cases, total_deaths, (cast(total_deaths as decimal)/total_cases)*100 as death_rate
from portfolio_project..covid_death_test_hospitalization
where continent is not null and total_cases is not null and total_deaths is not null 
order by death_rate desc


/* 1.3. After removing these irregular records and sort the data again by death_rate,
		I find that some countries have really high death rates at the beginning of the pandemic (>50%) and these rates gradually decrease over time. 
		Most of these countries are in Africa which has poor living standard and healthcare systems. 
		Some other countries in Europe and Asia (e.g. Ireland, Phillipines) also had high death rates early in the pandemic.
		However, given that these death rates were calculated based on a very small number of cases, there is little meaning in these figures.

		Factors attributable to the high death rates in these Africa countries at the start of the pandemic 
		could be attributable to the poor economic condition of the country and the lack of knowledge and experience in 
		proper treatment plans for Covid patients
		However, this is just a speculative explanation. All trends observed need further information before any conclusions can be made.
*/
select iso_code, continent, location, date, total_cases, total_deaths, (cast(total_deaths as decimal)/total_cases)*100 as death_rate
from portfolio_project..covid_death_test_hospitalization
where 
	continent is not null and total_cases is not null 
	and total_deaths is not null and total_cases> total_deaths
order by death_rate desc


/*	Latest death rates - Demographic factors
	Since death rates gradually reduced over time, the latest death rates are a better representative of the Covid impact.
	In this batch of code, I extract only the latest record for each country. It is also worth examining 
	some demographic factors that might help explain these high death rates.
	1. Extract latest records of death rates 
	2. Join with de demographics data to further investigate some demographics characteristics of these countries. 
   
    Please note that in practice, I would prefer to use SQL to extract data 
    then ultilise other softwares (SAS, Python or Stata) to conduct the analysis. 
    This is therefore just for the purpose of showcasing my skill in SQL.
	
	
	Analysis: 
	- After observing the death rate and some demographic factors, I found that these factors do not have any clear relationship 
	with death rates. 
	- Some factors are not present in the data but can help explain the death rate variation are (i) vaccination (which we will investigate later on),
	(ii) the start of the covid pandemic in a country, (iii) a country's Covid lockdown policy. 
	- To consider the impact of multiple factors on death rate across a big panel data, statistical analysis is necessary. This can be done easily 
	in SAS, Python or other statistical softwares. */

with cte as
	(
	select iso_code, continent, location, date, total_cases, total_deaths, (cast(total_deaths as decimal)/total_cases)*100 as death_rate
	from portfolio_project..covid_death_test_hospitalization
	where continent is not null and total_cases is not null and total_deaths is not null and total_cases>total_deaths 
	)

select 
		a.iso_code, a.location, a.continent, a.death_rate, a.date, 
		b.population, b.extreme_poverty, b.population_density, 
		b.gdp_per_capita, b.hospital_beds_per_thousand
from 
	(
		select 
			iso_code, location, continent, date, death_rate,
			row_number() over(partition by iso_code order by date desc) as rn 
		from cte
	) a
join 
	portfolio_project..covid_population_healthcare b
on a.iso_code = b.iso_code and a.date = b.date 
where rn = 1
order by death_rate desc;


--2. Total cases and total death vs. total population
create view view_death_population as
select 
		a.iso_code, a.location, a.date, 
		a.total_cases, a.total_deaths,b.population, b.population_density,
		(cast(a.total_deaths as decimal)/total_cases)*100 death_rate,
		(cast(a.total_cases as decimal)/b.population)*100 infection_rate
from 
		portfolio_project..covid_death_test_hospitalization a
join	
		portfolio_project..covid_population_healthcare b
on		a.iso_code = b.iso_code and a.date = b.date
where	total_cases is not null and total_deaths is not null and total_cases > total_deaths;

/*Extract only the latest record for each country since the latest infection rate is more 
representative of the final overall spread of Covid in a country. 
Sort the data by the infection rate in a descending order. 

Analysis: 
There are hardly any clear observed connection between infection rate and death rate. 
Again, when there are many factors interacting, we need some statistical analysis to
isolate and examine the impact of each factors. Some factors that might have a role 
here include: vaccination rate, whether a country can get vaccines early or have to wait long, 
the healcare system capacity, demographic factors (age, income).*/

select *
from 
	(
	select *, row_number() over(partition by iso_code order by date desc) rn
	from view_death_population
	) a
where rn =1
order by infection_rate desc;


-- 3. Vaccination, infection rate and death rate
create view view_Vaccine_death as
select 
		a.*, b.people_fully_vaccinated, b.people_vaccinated, b.total_vaccinations,
		(cast(b.people_fully_vaccinated as decimal)*100/a.population) fully_vac_rate,
		(cast(b.people_vaccinated as decimal)*100/a.population) vac_rate
from 
		view_death_population a
join	
		portfolio_project..Covid_vaccination b
on		a.iso_code = b.iso_code and a.date = b.date
where	total_cases is not null and total_deaths is not null and total_cases > total_deaths and continent is not null;

select *
from view_Vaccine_death
order by iso_code, date

/*similarly, extract only the latest record to see the impact of vaccination on death rate
Looking at the latest records, there seems to be no relationship between vaccination rate and death rate and infection rate. 
ONe possible reason is that country who get vaccination first will be able to contain the spread and limit the death rate 
and therefore end up with lower infection rate and lower death rate.
So, I will create a new column that show the first vaccination date of each country
*/
select a.*, b.date first_vac_date
From
	(
	select iso_code, location, infection_rate, death_rate, fully_vac_rate, vac_rate
	from 
		(
		select *, row_number() over(partition by iso_code order by date desc) rn
		from view_Vaccine_death
		where people_vaccinated is not null
		) c
	where rn =1 and vac_rate <100
	) a
left Join
	( 
	select iso_code, date, ROW_NUMBER() over(partition by iso_code order by date asc) rn, total_vaccinations
	from portfolio_project..Covid_vaccination
	where people_vaccinated is not null
	) b

on a.iso_code = b.iso_code and b.rn = 1
order by death_rate desc;

/*When sorting by death rate to observe the relationship between death rate and vaccination start date,
there exist little relationship between these two. Again, a statistical analysis would benefit to single out
impacts of those factors on the Covid death rate and infection rate 

I will use Python to examine this statistically*/
