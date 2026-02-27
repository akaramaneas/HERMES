# HERMES HEllenic Regional Model for the Electricity System
This repository includes all the necessary files to run HERMES

HERMES is an electricity model that models the Greek power sector, dissecting it in 21 regions: 10 mainland NUTS-2 regions and 11 major Greek insular grids, being characterised by high temporal, spatial and technological granularity.

Running HERMES requires installing the GLPK and CPLEX solvers. GLPK is an open solver, while CPLEX is also free for academic use, both used through a Python Command prompt (e.g. Miniconda)

**Steps to run the model and extract results:**

1. Save all the necessary files in the same folder. To run a scenario, a single Inputs folder is required.
2. Open the Python Command Prompt.
3. Enter the command "python convert.py". This command converts the multiple csv input files into an aggregated input .txt file, resembling the function of otoole.
4. Enter the command "glpsol -m hermes.txt -d data.txt --wlp input.lp --check". This command creates the .lp file that is required to solve the problem via CPLEX.
5. Enter the command "CPLEX". It opens the interactive CPLEX module.
6. Enter the command "read input.lp". It loads the problem file into CPLEX.
7. Enter the command "optimize", which solves the problem. For a faster solution before the optimisation command, you can select to use specifically the barrier method with no crossover. To do so, enter the commands "set lpmethod 4" & "set solutiontype 2" before optimising the problem.
8. Once CPLEX solves the problem, it displays an indicative message with the objective solution. Then you enter the command "write solution.sol". It writes the initial solution of the problem.
9. Then you have to quit CPLEX via the "quit" command.
10. Once you quit CPLEX, the solution file has to be converted into a .txt file. This is achieved through the command "python transform_31072013.py solution.sol out.txt".
11. Once the out.txt file is generated -it takes a couple of minutes-, enter the command "python Working_GR.py out.txt". It creates a folder with national and regional results in .csv files. The national results are further aggregated for better processing. The regional results are provided for each region modelled.
12. Enter the command "cd results" to change the directory to the results folder. In this folder, add the "dispatch_code_py" script.
13. Lastly, enter the command "python dispatch_code.py", which generates a dispatch file for each region and each year.
