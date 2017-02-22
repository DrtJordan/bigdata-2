##第一个参数是产品版本(默认是master)
## build             (默认为 build master)
## build master


if [ $# == 1 ]; then
	branch=$1
else
	branch=master
fi

basedir=$(cd "$(dirname "$0")"; pwd)
echo $basedir
cd $basedir
cd ..

mvn clean
git pull https://git.ws.netease.com/cms-analyzer/bigdata.git ${branch}
mvn package

cd weblogOffline/run/
sh build.sh ${branch}
