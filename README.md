# DS410_final_project


## Steps for obtaining raw dataset used in cluster mode
### DOWNLOAD DATASET HERE:
https://drive.google.com/file/d/1BUWjYVQseIe5vuLmv6aS4KzvVRezGODQ/view?usp=drive_link

*File size exceeded GitHub's limits, unable to store on our repository*

### OR DOWNLOAD WITH THIS LINK TO: [Water quality portal](https://www.waterqualitydata.us/#countrycode=US&mimeType=csv&sorted=no&providers=NWIS&providers=STEWARDS&providers=STORET)

![First step for obtaining raw dataset](img/step1_dataset.png)
![Second step for obtaining raw dataset](img/step2_dataset.png)
![Last step for obtaining raw dataset](img/step3_dataset.png)


## How to run: 
FINAL_DS410_Project_Cluster.py is the primary code for this project. It uses our final selected method of replacing NULL values with the average of each characteristic for its state. In other words, if we had a missing value of conductance from a sample in State College, we would replace it with the average for State College.

Alternative_DS410_Project_Mean_Method_1.py is a secondary code that shows the implementation of replacing NULL values with the average of each characterstic across the entire dataset. 

