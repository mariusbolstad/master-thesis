# master-thesis


Solstorm:
```bash
ssh solstorm-login.iot.ntnu.no -l mariumbo
scp /Users/mariusbolstad/VSCode/master-thesis/forecast_macro.py mariumbo@solstorm-login.iot.ntnu.no:~
scp /Users/mariusbolstad/VSCode/master-thesis/sol_test.R mariumbo@solstorm-login.iot.ntnu.no:~
screen
screen -ls
screen -rd <session number from screen -ls>
ssh compute-2-40
module load R
R CMD BATCH yourscript.R
cd ../../storage/users/mariumbo
```

