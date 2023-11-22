<h1>Построение ETL процесса</h1>

<h3>Описание задачи.</h3>
Разработать ETL процесс, получающий ежедневную выгрузку данных
(предоставляется за 3 дня), загружающий ее в хранилище данных и ежедневно
строящий отчет.

<h3>Выгрузка данных.</h3>
Ежедневно некие информационные системы выгружают три следующих
файла:
<ol>
<li>Список транзакций за текущий день. Формат – CSV.</li>
<li>Список терминалов полным срезом. Формат – XLSX.</li>
<li>Список паспортов, включенных в «черный список» - с накоплением с начала
месяца. Формат – XLSX.</li>
</ol>
Сведения о картах, счетах и клиентах хранятся в СУБД PostgreSQL.

<h3>Признаки мошеннических операций для построения отчета (FRAUD).</h3>
<ol>
<li>Совершение операции при просроченном или заблокированном паспорте.</li>
<li>Совершение операции при недействующем договоре.</li>
<li>Совершение операций в разных городах в течение одного часа.</li>
</ol>
