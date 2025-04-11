# Report transfer from Yandex Metrika (API) to MariaDB
## Setting up (Linux):

```shell
git clone https://github.com/TheMatrix2/Report-Transfer
```
```shell
cd Report-Transfer
```
_Environment settings in .env file_
```shell
cp .env.example .env
```
_Python, MariaDB and DB user configuration:_
```shell
chmod +x setup.sh
```
```shell
./setup.sh
```
_Schedule configuration for updates:_
```shell
chmod +x schedule.sh
```
```shell
./schedule.sh
```
