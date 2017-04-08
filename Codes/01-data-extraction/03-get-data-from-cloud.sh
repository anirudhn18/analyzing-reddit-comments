base="https://storage.googleapis.com/reddit_bigdataproject/new/subreddits0000000000"

echo 'creating a folder called datafiles to store the csvs'
rm -rf datafiles
mkdir datafiles
cd datafiles

for i in `seq 0 9`;
do
echo "
 

processing $i of 38"
wget $base'0'$i
done

for i in `seq 10 38`;
do
echo "


processing $i of 38"
wget $base$i
done