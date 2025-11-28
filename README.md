# Русификатор для игры Where Winds Meet

[![Steam Store Game](https://img.shields.io/badge/%D0%98%D0%B3%D1%80%D0%B0%20%D0%B2%20-%20Steam%20-%20Where%20Winds%20Meet?style=flat&logo=steam&logoColor=%23004B8D&label=%D0%A1%D1%82%D1%80%D0%B0%D0%BD%D0%B8%D1%86%D0%B0%20%D0%B8%D0%B3%D1%80%D1%8B%20%D0%B2&labelColor=%23FFFFFF&color=%23004B8D&link=https%3A%2F%2Fstore.steampowered.com%2Fapp%2F3564740%2FWhere_Winds_Meet%2F)](https://store.steampowered.com/app/3564740/Where_Winds_Meet/) [![MIT License](https://img.shields.io/badge/%D0%9B%D0%B8%D1%86%D0%B5%D0%BD%D0%B7%D0%B8%D1%8F%20-%20MIT%20-%203?style=flat&logo=data%3Aimage%2Fsvg%2Bxml%3Bbase64%2CPD94bWwgdmVyc2lvbj0iMS4wIiBlbmNvZGluZz0iaXNvLTg4NTktMSI%2FPg0KPCFET0NUWVBFIHN2ZyBQVUJMSUMgIi0vL1czQy8vRFREIFNWRyAxLjEvL0VOIiAiaHR0cDovL3d3dy53My5vcmcvR3JhcGhpY3MvU1ZHLzEuMS9EVEQvc3ZnMTEuZHRkIj4NCjxzdmcgZmlsbD0iIzAwMDAwMCIgaGVpZ2h0PSI4MDBweCIgd2lkdGg9IjgwMHB4IiB2ZXJzaW9uPSIxLjEiIGlkPSJDYXBhXzEiIHhtbG5zPSJodHRwOi8vd3d3LnczLm9yZy8yMDAwL3N2ZyIgeG1sbnM6eGxpbms9Imh0dHA6Ly93d3cudzMub3JnLzE5OTkveGxpbmsiIA0KCSB2aWV3Qm94PSIwIDAgMjI0LjcyNiAyMjQuNzI2IiB4bWw6c3BhY2U9InByZXNlcnZlIj4NCjxwYXRoIGQ9Ik0yMjMuNjEyLDEwOS41ODZMMTkwLjUsMzUuNzUyYy0wLjAwMS0wLjAwMi0wLjAwMi0wLjAwNC0wLjAwMy0wLjAwNmwtMC4wNDMtMC4wOTZjLTAuMTExLTAuMjQ3LTAuMjQzLTAuNDc5LTAuMzg5LTAuNw0KCWMtMC4wMzktMC4wNi0wLjA4Ni0wLjExNC0wLjEyNy0wLjE3MWMtMC4xMzEtMC4xODItMC4yNzItMC4zNTMtMC40MjQtMC41MTRjLTAuMDYtMC4wNjMtMC4xMjEtMC4xMjUtMC4xODQtMC4xODUNCgljLTAuMTY4LTAuMTU5LTAuMzQ1LTAuMzA2LTAuNTMzLTAuNDRjLTAuMDQ5LTAuMDM2LTAuMDk0LTAuMDc2LTAuMTQ1LTAuMTFjLTAuMjM2LTAuMTU2LTAuNDg2LTAuMjktMC43NDYtMC40MDUNCgljLTAuMDc0LTAuMDMzLTAuMTUxLTAuMDU3LTAuMjI3LTAuMDg2Yy0wLjIwNC0wLjA3OC0wLjQxMS0wLjE0My0wLjYyNS0wLjE5NGMtMC4wODgtMC4wMjEtMC4xNzUtMC4wNDItMC4yNjQtMC4wNTgNCgljLTAuMjkzLTAuMDU0LTAuNTkxLTAuMDktMC44OTYtMC4wOWgtNTguNDI4Yy0xLTMuMTExLTIuOTQzLTUuNzk0LTUuNDg1LTcuNzM3di03LjI2MmMwLTIuNzYxLTIuMjM5LTUtNS01aC05LjMzNA0KCWMtMi43NjEsMC01LDIuMjM5LTUsNXY3LjM4MWMtMi40NjUsMS45MzMtNC4zNDcsNC41NzItNS4zMjYsNy42MTlIMzguNzM0Yy0wLjMwNSwwLTAuNjAyLDAuMDM3LTAuODk2LDAuMDkNCgljLTAuMDksMC4wMTYtMC4xNzYsMC4wMzctMC4yNjQsMC4wNThjLTAuMjE0LDAuMDUxLTAuNDIyLDAuMTE2LTAuNjI1LDAuMTk0Yy0wLjA3NiwwLjAyOS0wLjE1MywwLjA1My0wLjIyNywwLjA4Ng0KCWMtMC4yNiwwLjExNS0wLjUxLDAuMjQ4LTAuNzQ2LDAuNDA1Yy0wLjA1MSwwLjAzNC0wLjA5NiwwLjA3NC0wLjE0NSwwLjExYy0wLjE4OCwwLjEzNC0wLjM2NSwwLjI4LTAuNTMzLDAuNDQNCgljLTAuMDY0LDAuMDYtMC4xMjQsMC4xMjItMC4xODUsMC4xODVjLTAuMTUzLDAuMTYxLTAuMjkzLDAuMzMzLTAuNDI0LDAuNTE0Yy0wLjA0MiwwLjA1OC0wLjA4OCwwLjExMi0wLjEyNywwLjE3MQ0KCWMtMC4xNDYsMC4yMjEtMC4yNzgsMC40NTMtMC4zODksMC43bC0wLjA0MywwLjA5NmMtMC4wMDEsMC4wMDItMC4wMDIsMC4wMDQtMC4wMDMsMC4wMDZMMC44OTcsMTA5Ljg0OA0KCUMwLjMzNSwxMTAuNjU3LDAsMTExLjYzNiwwLDExMi42OTZjMCwyMS4zNTgsMTcuMzc2LDM4LjczNCwzOC43MzQsMzguNzM0YzIxLjM1OCwwLDM4LjczNC0xNy4zNzYsMzguNzM0LTM4LjczNA0KCWMwLTEuMDYtMC4zMzQtMi4wMzgtMC44OTctMi44NDdMNDYuNDU2LDQyLjY5Nmg1MC45ODZjMS4wMDcsMi45MDUsMi44MzEsNS40MjQsNS4yMDQsNy4yODZ2MTI1LjM4MUg1OC4zMDgNCgljLTEuODcxLDAtMy41ODYsMS4wNDUtNC40NDQsMi43MDhMNDAuMTEsMjA0LjczOGMtMC43OTksMS41NS0wLjczMywzLjQwNSwwLjE3NCw0Ljg5NGMwLjkwOCwxLjQ4OSwyLjUyNSwyLjM5OCw0LjI3LDIuMzk4aDEzNS41MTkNCgljMC4wMDYtMC4wMDEsMC4wMTIsMCwwLjAyLDBjMi43NjEsMCw1LTIuMjM5LDUtNWMwLTAuOTgtMC4yODItMS44OTQtMC43NjktMi42NjVsLTEzLjU2My0yNi4yOTQNCgljLTAuODU4LTEuNjYzLTIuNTczLTIuNzA4LTQuNDQ0LTIuNzA4SDEyMS45OFY1MC4xMDFjMi40NDktMS44NzMsNC4zMzQtNC40MzYsNS4zNjMtNy40MDVoNTAuODI4bC0zMC40NzUsNjcuOTU0DQoJYy0wLjAwOCwwLjAxOC0wLjAxMiwwLjAzNi0wLjAyLDAuMDU0Yy0wLjAyMywwLjA1NC0wLjA0LDAuMTEtMC4wNjIsMC4xNjVjLTAuMDksMC4yMjktMC4xNjIsMC40NjMtMC4yMTcsMC42OTkNCgljLTAuMDE0LDAuMDYyLTAuMDMyLDAuMTIyLTAuMDQ0LDAuMTg0Yy0wLjA1NSwwLjI4Ny0wLjA4MywwLjU3Ni0wLjA4NywwLjg2NmMwLDAuMDI3LTAuMDA4LDAuMDUxLTAuMDA4LDAuMDc4DQoJYzAsMjEuMzU4LDE3LjM3NiwzOC43MzQsMzguNzM0LDM4LjczNHMzOC43MzQtMTcuMzc2LDM4LjczNC0zOC43MzRDMjI0LjcyNiwxMTEuNTE0LDIyNC4yOTgsMTEwLjQ0MiwyMjMuNjEyLDEwOS41ODZ6DQoJIE02NC42NDYsMTA3LjY5NkgxMi44MjJsMjUuOTEyLTU3Ljc4MUw2NC42NDYsMTA3LjY5NnogTTExOC4yMjYsMzcuNTNjMCwzLjIxNy0yLjYxNyw1LjgzMy01LjgzMyw1LjgzMw0KCWMtMy4yMTcsMC01LjgzNC0yLjYxNy01LjgzNC01LjgzM2MwLTMuMjE3LDIuNjE3LTUuODMzLDUuODM0LTUuODMzQzExNS42MDksMzEuNjk2LDExOC4yMjYsMzQuMzEzLDExOC4yMjYsMzcuNTN6IE0xNTkuOTgsMTA3LjY5Ng0KCWwyNS45MTMtNTcuNzgxbDI1LjkxMiw1Ny43ODFIMTU5Ljk4eiIvPg0KPC9zdmc%2B&labelColor=white&color=CC2F87)](https://github.com/DOG729/wwm_russian/blob/main/LICENSE) [![Boosty](https://img.shields.io/badge/Boosty%20-%20Boosty%20-%20Boosty?style=flat&logo=boosty&logoColor=orange&label=%D0%9F%D0%BE%D0%B4%D0%B4%D0%B5%D1%80%D0%B6%D0%B0%D1%82%D1%8C%20%D0%BD%D0%B0&labelColor=white&color=orange&link=https%3A%2F%2Fboosty.to%2Fdog729)](https://boosty.to/dog729)

## ℹ️ О проекте

Это **неофициальная** русская локализация для MMO игры [Where Winds Meet](https://store.steampowered.com/app/3564740/Where_Winds_Meet/) от Everstone Studio и NetEase Games. Проект создан сообществом и не связан с официальными разработчиками игры.

**Where Winds Meet** — это эпическая open-world action-adventure RPG в жанре Wuxia, действие которой происходит в Китае X века. Игра доступна бесплатно в Steam и поддерживает одиночную игру, кооператив и PvP.

---

## 📚 Документация

- **[`docs/tags.md`](docs/tags.md)** — описание тегов и форматирования, используемых в игре (ссылки, переменные, цветовое оформление);
- **[`_soft/README.md`](_soft/README.md)** — документация по GUI-инструменту для распаковки/запаковки ресурсов и работы с текстами игры;
- **[`docs/localization.md`](docs/localization.md)** — правила перевода для **[`/translation_ru.tsv`](/translation_ru.tsv)**.

## 🔗 Ссылки

- [Сравнение файлов локализации](https://dog729.github.io/wwm_russian/www/comparisons.html)
- [Страница обсуждения перевода на сайте Zone of Games](https://forum.zoneofgames.ru/topic/80635-where-winds-meet/)
- [Google Таблица со словарём для перевода](https://docs.google.com/spreadsheets/d/1tTemjK3A1iD7sbvxPfZ8tk6xjOGZpGzdsGWl-TAgBtc/edit?usp=sharing)
- [Руководство Steam](https://steamcommunity.com/sharedfiles/filedetails/?id=3609472595)


## Дополнительная информация
<details open>
<summary>Credits</summary>

* [Dontaz](https://github.com/Dontaz) - Публикация, Редактирование публичных материалов, Шпионаж, Продвижение, Постоянные гифки с котиками
* [Claymore0098](https://github.com/Kirito0098) - Самый большой вклад по нейропереводу для черновика. 
* [AleksejBelov](https://github.com/grifon102)
* [ZoG Community](https://forum.zoneofgames.ru/topic/80635-where-winds-meet)
* [Contributors](https://github.com/DOG729/wwm_russian/graphs/contributors)
* Обновление следует... 😇 

</details>
