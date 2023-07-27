This change
conda activate test
kill -9 $(lsof -ti:8899)
cd credit_scoring/
feast teardown
feast apply
feast ui -p 8899 &
