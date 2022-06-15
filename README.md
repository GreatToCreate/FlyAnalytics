
# FlyAnalytics
FlyAnalytics is an analytics utility meant to pull information from the Fly Dangerous Steam Leaderboards and sink that information into the database underlying FlyAPI to:

 - Populate the API leaderboard-related endpoints to support the discord bot FlyBot
 - Enable historical course data analysis
 - Allow a lot of cool future-state endpoints to supply data to various web and unity charting utilities

This project supports the broader [Fly Dangerous](https://github.com/jukibom/FlyDangerous) project.

**If you're looking to contribute, consider putting that effort towards either FlyAPI or FlyBot as this component is a very temporary and dirty implementation that was meant to get FlyBot operational as quickly as possible. This project will eventually migrate into using [Dagster](https://dagster.io/) as its pipeline orchestrator and will be a near-entire re-write.**

## Setting Up the Project
1. Ensure you have a FlyAPI instance setup and running
2. Clone or download this repository locally  
3. Create a [virtual environment](https://docs.python.org/3/library/venv.html) to install dependencies into  
4. Activate your virtual environment and install dependencies using pip install -r requirements.txt from the project root

## Running the Project

### Manual Setup
1. Environmental variables to set:  
   - DB_URL - the [SQLAlchemy connection string](https://docs.sqlalchemy.org/en/14/core/engines.html) used to connect to the underlying database instance
   - UPDATE_FREQUENCY_MIN - the frequency in which the function run_leaderboards_job should be executed. 
	   - Be aware that this function currently generates 16 leaderboards api calls + 50 steam user api calls per run, so ensure that you're operating within steam's API limits 
	   - Recommended value = 15
2. Within your activated virtual environment, execute main.py

###  Docker Setup and Run [Recommended]
**Example docker-compose file will be included in FlyAPI until all images are available in a github container registry**
