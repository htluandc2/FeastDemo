This change
This change 2
This change 2
conda activate test
kill -9 $(lsof -ti:8899)
cd credit_scoring/
feast teardown
feast apply
feast ui -p 8899 &
