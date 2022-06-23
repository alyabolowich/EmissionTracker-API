Title: API update for consumption-based EU GHG emissions
Author: Alya Bolowich
Date: 26 June 2022
About: The EmissionTracker API has been updated to include the environmental and social stressors (emissions) from the 49 countries in EXIOBASE. This project builds on the work of the Bachelor Semester Project 3, which created an initial vesion.

This directory contains the following files:

    1) functions.py - python file that contains all the functons used in this project
    2) data_download.py - python file that will execute upon update
    3) documentation folder - contains the user and developer documentation as HTML files
    4) app folder - contains the app.py and index.html files used for the API
    5) current_doi.txt - stores EXIOBASE's DOI provided by Zenodo

Autoamatic updates:

The API shall make a monthly request to the Zenodo repository in which EXIOBASE is hosted. The program is activated using the job scheduler cron jobs. The following code snippet should be used for this automation process (NB!: path to file corresponds to system of author and needs to be reinstantiated for another user):

0 9 1 * * python3 /code/data_download.py > /tmp/program.out 2> /tmp/program.err
