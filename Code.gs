function getDataBySql(
    strSQL = "SELECT * FROM `ads_collection` WHERE `source` = 'facebook'"
) {
    try {
        // === 1. Надсилаємо POST запит до PHP бекенду ===
        const url = "https://api.trll-notif.com.ua/adsreportcollector/core.php";

        const options = {
            method: "post",
            contentType: "application/json",
            payload: JSON.stringify({ sql: strSQL }),
            muteHttpExceptions: true,
        };

        console.log("🔍 Sending request to database API...");
        const response = UrlFetchApp.fetch(url, options);

        // Проверяем HTTP статус
        if (response.getResponseCode() !== 200) {
            return {
                error: `HTTP ${response.getResponseCode()}: Сервер базы данных недоступен`,
            };
        }

        const responseText = response.getContentText();
        if (!responseText || responseText.trim() === "") {
            return { error: "Пустой ответ от сервера базы данных" };
        }

        let json;
        try {
            json = JSON.parse(responseText);
        } catch (parseError) {
            return {
                error: "Неверный формат ответа от сервера: " + parseError.message,
            };
        }

        // === 2. Обробка відповіді від бекенду ===
        if (json.error) {
            return { error: json.error };
        }

        if (!json || !Array.isArray(json) || json.length === 0) {
            return { error: "empty data" };
        }

        // === 3. Повертаємо дані напряму (БЕЗ створення листів) ===
        const data = json;

        // Перетворюємо в формат, який очікує функція parseDbResults
        if (typeof data[0] === "object" && !Array.isArray(data[0])) {
            // Якщо це масив об'єктів, перетворюємо в формат [headers, ...rows]
            const headers = Object.keys(data[0]);
            const rows = data.map((row) => headers.map((h) => row[h]));
            console.log("✅ Successfully processed", rows.length, "data rows");
            return [headers, ...rows];
        } else {
            // Якщо вже масив масивів
            console.log("✅ Successfully received", data.length, "data rows");
            return data;
        }
    } catch (error) {
        console.error("❌ Ошибка в getDataBySql:", error);
        return { error: "Ошибка подключения: " + error.message };
    }
}

/**
 * УЛУЧШЕННАЯ функция для парсинга названия кампании - точнее извлекает байера
 */
function parseCampaignName(fullName) {
    const result = {
        article: "",
        productName: "",
        buyer: "",
        source: "",
        account: "",
    };

    if (!fullName || typeof fullName !== "string") {
        return result;
    }

    try {
        console.log("🔍 Parsing campaign name:", fullName);

        // Ищем артикул в начале (буквы + цифры)
        const articleMatch = fullName.match(/^([A-Z]+\d+)/);
        if (articleMatch) {
            result.article = articleMatch[1];
        }

        // Разделяем по " | "
        const parts = fullName.split(" | ");
        console.log("📝 Campaign parts:", parts);

        if (parts.length >= 2) {
            // ВТОРОЙ элемент - это БАЙЕР (точно!)
            result.buyer = parts[1].trim();
            console.log("👤 Found buyer:", result.buyer);

            if (parts.length >= 3) {
                // Третий элемент содержит источник + аккаунт
                const sourceAccountPart = parts[2].trim();

                // Парсим источник и аккаунт
                // Пример: "TikTok WL1 Akk1.5" -> источник: "TikTok", аккаунт: "WL1"
                const sourceMatch = sourceAccountPart.match(
                    /^(TikTok|Facebook|Instagram|Google)\s*(.*)/i
                );
                if (sourceMatch) {
                    result.source = sourceMatch[1];
                    const accountPart = sourceMatch[2];

                    // Ищем аккаунт (обычно VL + цифры или WL + цифры)
                    const accountMatch = accountPart.match(/\b(VL\d+|WL\d+|[A-Z]+\d+)\b/);
                    if (accountMatch) {
                        result.account = accountMatch[1];
                    }
                } else {
                    result.source = sourceAccountPart;
                }
            }
        }

        // Извлекаем название товара (между артикулом и первым " | ")
        if (parts.length >= 1) {
            const firstPart = parts[0];
            const productMatch = firstPart.replace(result.article, "").trim();
            if (productMatch.startsWith(" ")) {
                result.productName = productMatch
                    .substring(1)
                    .replace(/\s*-\s*$/, "")
                    .trim();
            }
        }
    } catch (e) {
        console.log("❌ Ошибка парсинга названия кампании:", fullName, e);
    }

    console.log("✅ Parsed campaign info:", result);
    return result;
}

/**
 * Функция для веб-приложения
 */
function doGet() {
    return HtmlService.createTemplateFromFile("График HTML")
        .evaluate()
        .setXFrameOptionsMode(HtmlService.XFrameOptionsMode.ALLOWALL)
        .setTitle("Аналитика Facebook Ads");
}

/**
 * Функция для подключения внешних файлов
 */
function include(filename) {
    return HtmlService.createHtmlOutputFromFile(filename).getContent();
}

/**
 * Функция для сортировки метрик по порядку байеров
 */
function sortMetricsByBuyers(metrics, buyers, buyerGroupsMap) {
    const sortedMetrics = [];

    buyers.forEach((buyer) => {
        if (buyerGroupsMap[buyer]) {
            Array.from(buyerGroupsMap[buyer]).forEach((group) => {
                if (metrics.includes(group) && !sortedMetrics.includes(group)) {
                    sortedMetrics.push(group);
                }
            });
        }
    });

    metrics.forEach((metric) => {
        if (!sortedMetrics.includes(metric)) {
            sortedMetrics.push(metric);
        }
    });

    return sortedMetrics.join("\n");
}

/**
 * Функция для сортировки видео по порядку байеров
 */
function sortVideosByBuyers(buyers, buyerVideosMap, globalVideos) {
    const sortedVideos = [];

    buyers.forEach((buyer) => {
        if (buyerVideosMap[buyer]) {
            Array.from(buyerVideosMap[buyer]).forEach((video) => {
                if (!sortedVideos.includes(video)) {
                    sortedVideos.push(video);
                }
            });
        }
    });

    Array.from(globalVideos).forEach((video) => {
        if (!sortedVideos.includes(video)) {
            sortedVideos.push(video);
        }
    });

    return sortedVideos.join("\n");
}

/**
 * Функция для сортировки сайтов по порядку байеров
 */
function sortSitesByBuyers(buyers, buyerSitesMap, globalSites) {
    const sortedSites = [];

    buyers.forEach((buyer) => {
        if (buyerSitesMap[buyer]) {
            Array.from(buyerSitesMap[buyer]).forEach((site) => {
                if (!sortedSites.includes(site)) {
                    sortedSites.push(site);
                }
            });
        }
    });

    Array.from(globalSites).forEach((site) => {
        if (!sortedSites.includes(site)) {
            sortedSites.push(site);
        }
    });

    return sortedSites.join("\n");
}

/**
 * Функция для открытия веб-приложения аналитики из меню
 */
function buildChartForSelectedArticle() {
    try {
        // Получаем активную ячейку
        const sheet = SpreadsheetApp.getActiveSheet();
        const activeCell = sheet.getActiveCell();
        const article = activeCell.getValue();

        if (!article || typeof article !== "string" || article.trim() === "") {
            SpreadsheetApp.getUi().alert(
                "Ошибка!",
                "Выберите ячейку с артикулом для анализа.",
                SpreadsheetApp.getUi().ButtonSet.OK
            );
            return;
        }

        // Открываем веб-интерфейс
        openAnalyticsWebApp();
    } catch (error) {
        console.error("Ошибка в buildChartForSelectedArticle:", error);
        SpreadsheetApp.getUi().alert(
            "Ошибка!",
            "Произошла ошибка: " + error.toString(),
            SpreadsheetApp.getUi().ButtonSet.OK
        );
    }
}

/**
 * Функция для открытия веб-приложения аналитики
 */
function openAnalyticsWebApp() {
    try {
        // URL вашего веб-приложения (замените на ваш реальный URL после развертывания)
        const webAppUrl =
            "https://script.google.com/macros/s/AKfycbwHzc7rzUfwFxdJmS8HPd09O-FTPVtuu3L2v_I2jG1pgSChnfjvvSTEzuQ1iKJQPRw/exec";

        // Создаем HTML для открытия в новой вкладке
        const html = `
      <script>
        window.open('${webAppUrl}', '_blank');
        google.script.host.close();
      </script>
      <p>Открываем аналитику в новой вкладке...</p>
    `;

        const htmlOutput = HtmlService.createHtmlOutput(html)
            .setWidth(300)
            .setHeight(100);

        SpreadsheetApp.getUi().showModalDialog(htmlOutput, "Открытие аналитики...");
    } catch (error) {
        console.error("Ошибка при открытии веб-приложения:", error);
        SpreadsheetApp.getUi().alert(
            "Ошибка!",
            "Не удалось открыть веб-приложение:\n" + error.toString(),
            SpreadsheetApp.getUi().ButtonSet.OK
        );
    }
}

/**
 * Основная функция для построения аналитики - для веб-интерфейса
 */
function buildChartForArticle(article, periodStart, periodEnd) {
    console.log("🔥 =================================");
    console.log("🔥 НАЧАЛО ФУНКЦИИ buildChartForArticle");
    console.log("🔥 Артикул:", article);
    console.log("🔥 Период с:", periodStart);
    console.log("🔥 Период до:", periodEnd);
    console.log("🔥 =================================");

    try {
        // ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
        function formatValueByRow(value, rowIndex) {
            // Для названий рекламы (индекс 17), URL (18) и бюджета (19) возвращаем как строку
            if (rowIndex === 17 || rowIndex === 18 || rowIndex === 19) {
                return value ? String(value).trim() : "";
            }

            const strVal = String(value || "").replace(",", ".");
            const num = parseFloat(strVal);
            if (isNaN(num)) return value ? String(value) : "";

            switch (rowIndex) {
                case 11:
                case 13:
                case 15:
                case 16:
                    return num.toFixed(2).replace(".", ",");
                case 12:
                    return num.toFixed(2).replace(".", ",") + "%";
                case 14:
                    return String(Math.floor(num));
                default:
                    return num.toFixed(2).replace(".", ",");
            }
        }

        function sumMultilineValues(valuesArray) {
            if (!Array.isArray(valuesArray)) return 0;

            let totalSum = 0;
            valuesArray.forEach((val) => {
                if (val !== undefined && val !== null && val !== "") {
                    const lines = String(val).split("\n");
                    lines.forEach((line) => {
                        const trimmedLine = line.trim();
                        if (trimmedLine !== "") {
                            const num = Number(trimmedLine) || 0;
                            totalSum += num;
                        }
                    });
                }
            });

            return totalSum;
        }

        function processDayValues(arr, rowIndex) {
            if (!Array.isArray(arr)) {
                return "";
            }

            // Фильтруем пустые значения и убираем дубликаты для строковых полей
            let valuesToConvert;
            if (rowIndex === 18 || rowIndex === 19) {
                // URL и Групповой бюджет - убираем дубликаты
                valuesToConvert = Array.from(
                    new Set(arr.filter((v) => v !== undefined && v !== null && v !== ""))
                );
            } else if (rowIndex === 17) {
                // Название рекламы - убираем дубликаты и фильтруем пустые
                valuesToConvert = Array.from(
                    new Set(
                        arr.filter(
                            (v) =>
                                v !== undefined &&
                                v !== null &&
                                v !== "" &&
                                String(v).trim() !== ""
                        )
                    )
                );
            } else {
                // Числовые поля - оставляем все значения
                valuesToConvert = arr.filter(
                    (v) => v !== undefined && v !== null && v !== ""
                );
            }

            return valuesToConvert
                .map((v) => formatValueByRow(v, rowIndex))
                .join("\n");
        }

        function calculateRating(cpl, ratingThreshold) {
            if (cpl === 0 || isNaN(cpl) || ratingThreshold === 0) return "";

            const percentage = (cpl / ratingThreshold) * 100;

            if (percentage <= 35) return "A";
            else if (percentage <= 65) return "B";
            else if (percentage <= 90) return "C";
            else return "D";
        }

        function parseSeason(seasonEmoji) {
            if (!seasonEmoji || seasonEmoji.trim() === "") return "Неизвестно";

            const seasonString = seasonEmoji.trim();

            // Проверяем на всесезон (все 4 эмодзи)
            if (
                seasonString.includes("☀️") &&
                seasonString.includes("🍁") &&
                seasonString.includes("❄️") &&
                seasonString.includes("🌱")
            ) {
                return "Всесезон";
            }

            const seasons = [];
            if (seasonString.includes("☀️")) seasons.push("Лето");
            if (seasonString.includes("🍁")) seasons.push("Осень");
            if (seasonString.includes("❄️")) seasons.push("Зима");
            if (seasonString.includes("🌱")) seasons.push("Весна");

            return seasons.length > 0 ? seasons.join(", ") : "Неизвестно";
        }

        // УЛУЧШЕННАЯ функция получения данных из БД с понятными ошибками
        function getDataFromDatabase(sqlQuery) {
            try {
                console.log("Executing SQL query...");
                const result = getDataBySql(sqlQuery);

                // Проверяем на ошибки от API
                if (result && typeof result === "object" && result.error) {
                    if (result.error === "empty data") {
                        throw new Error("EMPTY_DATA");
                    }
                    throw new Error(
                        `🚨 Ошибка API базы данных!\n\nКод ошибки: ${result.error}\n\nОбратитесь к администратору системы.`
                    );
                }

                // Проверяем, что получили массив данных
                if (!Array.isArray(result)) {
                    throw new Error(
                        "🔧 Неверный формат данных!\n\nПолучен неожиданный тип данных от сервера.\nОбратитесь к разработчику."
                    );
                }

                if (result.length === 0) {
                    throw new Error("EMPTY_DATA");
                }

                console.log("✅ Data received successfully:", result.length, "rows");
                return result;
            } catch (error) {
                console.error("❌ Error getting data from database:", error);
                throw error;
            }
        }

        function parseDbResults(data) {
            if (!data || data.length < 2) return [];

            const headers = data[0];
            const rows = data.slice(1);

            return rows.map((row) => {
                const obj = {};
                headers.forEach((header, index) => {
                    obj[header] = row[index];
                });
                return obj;
            });
        }

        if (!article || article.trim() === "") {
            console.log("❌ Пустой артикул");
            throw new Error(
                "📝 Артикул не может быть пустым!\n\nВведите корректный артикул для анализа."
            );
        }

        // Проверяем формат артикула
        article = article.trim().toUpperCase();
        console.log("📝 Обработанный артикул:", article);

        if (article.length < 3) {
            console.log("❌ Слишком короткий артикул:", article);
            throw new Error(
                `📝 Артикул "${article}" слишком короткий!\n\nАртикул должен содержать минимум 3 символа.`
            );
        }

        console.log("🚀 Starting analysis for article:", article);

        // Проверяем период
        let periodChosen = false;
        console.log(
            "🔍 Received period params - Start:",
            periodStart,
            "End:",
            periodEnd
        );
        console.log(
            "🔍 Period types - Start:",
            typeof periodStart,
            "End:",
            typeof periodEnd
        );

        // Проверяем наличие дат
        const hasStartDate = periodStart && periodStart.trim() !== "";
        const hasEndDate = periodEnd && periodEnd.trim() !== "";

        if (hasStartDate || hasEndDate) {
            periodChosen = true;
            console.log("📅 Period filter will be applied");
        } else {
            console.log("⚠️ No dates selected - showing all data");
        }

        // Получаем данные из Списoк2.0 (если доступны)
        let maxCPLThreshold = 3.5;
        let status = "Активный";
        let stock = "Не указано";
        let stockDays = "Не указано";
        let season = "Неизвестно";
        let category = "Не указана";
        let efficiencyZoneFormatted = {
            value: "--,--%",
            backgroundColor: "#f3f3f3",
            fontColor: "#666666",
        };
        let zoneABFormatted = "-";
        let zoneACFormatted = "-";
        let zoneADFormatted = "-";
        let zoneAEFormatted = "-";

        try {
            const ss = SpreadsheetApp.getActiveSpreadsheet();
            const sheetKapy = ss.getSheetByName("Списoк2.0");

            if (sheetKapy) {
                console.log("📊 Reading data from Списoк2.0 sheet...");
                const kapyData = sheetKapy.getDataRange().getValues();
                let articleRow = null;

                for (let i = 0; i < kapyData.length; i++) {
                    const cellValue = String(kapyData[i][1] || "").trim();
                    if (cellValue === article.trim()) {
                        articleRow = i + 1;
                        break;
                    }
                }

                // ПРОВЕРКА 1: Артикул существует
                if (!articleRow) {
                    console.log("❌ Article not found in Списoк2.0");
                    throw new Error(
                        `📝 Неверный артикул!\n\nАртикул "${article}" не найден в системе.\n\nПроверьте правильность написания артикула.`
                    );
                }

                // ПРОВЕРКА 2: Разрешение на просмотр (колонка BQ = столбец 69)
                const permissionValue = sheetKapy.getRange(articleRow, 69).getValue();
                console.log("🔐 Checking permission for article:", article, "Permission value:", permissionValue);

                if (permissionValue !== 1 && permissionValue !== "1") {
                    console.log("❌ No permission to view article:", article);
                    throw new Error(
                        `🔒 Нет разрешения на просмотр!\n\nДоступ к артикулу "${article}" ограничен.\n\nОбратитесь к администратору для получения разрешения.`
                    );
                }

                console.log("✅ Article found and permission granted:", article);

                if (articleRow) {
                    console.log("✅ Found article in Списoк2.0 at row:", articleRow);
                    const rawAB = sheetKapy.getRange(articleRow, 28).getValue();
                    const rawAF = sheetKapy.getRange(articleRow, 32).getValue();

                    // Проверяем AB (колонка 28) - приоритет
                    if (rawAB !== null && rawAB !== undefined && rawAB !== "" && !isNaN(rawAB) && Number(rawAB) > 0) {
                        maxCPLThreshold = Number(rawAB);
                    }
                    // Если AB пустая, проверяем AF (колонка 32)
                    else if (rawAF !== null && rawAF !== undefined && rawAF !== "" && !isNaN(rawAF) && Number(rawAF) > 0) {
                        maxCPLThreshold = Number(rawAF);
                    }
                    // Если обе пустые - константа 3.5
                    else {
                        maxCPLThreshold = 3.5;
                    }

                    status = String(
                        sheetKapy.getRange(articleRow, 4).getValue() || "Активный"
                    ).trim();
                    const stockValue = sheetKapy.getRange(articleRow, 34).getValue();
                    const stockDaysValue = sheetKapy.getRange(articleRow, 33).getValue();
                    const seasonEmoji = String(
                        sheetKapy.getRange(articleRow, 39).getValue() || ""
                    ).trim();
                    const categoryValue = String(
                        sheetKapy.getRange(articleRow, 44).getValue() || ""
                    ).trim();

                    stock =
                        stockValue !== null && stockValue !== undefined && stockValue !== ""
                            ? String(stockValue)
                            : "Не указано";
                    stockDays =
                        stockDaysValue !== null &&
                            stockDaysValue !== undefined &&
                            stockDaysValue !== ""
                            ? String(stockDaysValue)
                            : "Не указано";
                    season = parseSeason(seasonEmoji);
                    category = categoryValue || "Не указана";

                    // Зоны эффективности
                    const efficiencyZoneValue = sheetKapy
                        .getRange(articleRow, 27)
                        .getValue();
                    const zoneAB = sheetKapy.getRange(articleRow, 28).getValue();
                    const zoneAC = sheetKapy.getRange(articleRow, 29).getValue();
                    const zoneAD = sheetKapy.getRange(articleRow, 30).getValue();
                    const zoneAE = sheetKapy.getRange(articleRow, 31).getValue();

                    // ЧИТАЕМ ЦВЕТА ИЗ ЯЧЕЙКИ AA (колонка 27) - ДИНАМИЧЕСКИ!
                    const efficiencyZoneCell = sheetKapy.getRange(articleRow, 27);
                    let zoneBackgroundColor = null;
                    let zoneFontColor = null;

                    try {
                        // Получаем цвет фона ячейки
                        zoneBackgroundColor = efficiencyZoneCell.getBackground();
                        // Получаем цвет шрифта ячейки
                        zoneFontColor = efficiencyZoneCell.getFontColor();
                        console.log(
                            "🎨 Zone colors from sheet - Background:",
                            zoneBackgroundColor,
                            "Font:",
                            zoneFontColor
                        );
                    } catch (colorError) {
                        console.log("⚠️ Error reading cell colors:", colorError);
                        zoneBackgroundColor = "#f3f3f3";
                        zoneFontColor = "#666666";
                    }

                    efficiencyZoneFormatted =
                        efficiencyZoneValue !== null &&
                            efficiencyZoneValue !== undefined &&
                            efficiencyZoneValue !== ""
                            ? (Number(efficiencyZoneValue) * 100)
                                .toFixed(2)
                                .replace(".", ",") + "%"
                            : "--,--%";

                    zoneABFormatted =
                        zoneAB !== null && zoneAB !== undefined && zoneAB !== ""
                            ? Number(zoneAB).toFixed(2).replace(".", ",")
                            : "-";
                    zoneACFormatted =
                        zoneAC !== null && zoneAC !== undefined && zoneAC !== ""
                            ? Number(zoneAC).toFixed(2).replace(".", ",")
                            : "-";
                    zoneADFormatted =
                        zoneAD !== null && zoneAD !== undefined && zoneAD !== ""
                            ? Number(zoneAD).toFixed(2).replace(".", ",")
                            : "-";
                    zoneAEFormatted =
                        zoneAE !== null && zoneAE !== undefined && zoneAE !== ""
                            ? Number(zoneAE).toFixed(2).replace(".", ",")
                            : "-";

                    // Сохраняем цвета для передачи в интерфейс
                    efficiencyZoneFormatted = {
                        value:
                            efficiencyZoneValue !== null &&
                                efficiencyZoneValue !== undefined &&
                                efficiencyZoneValue !== ""
                                ? (Number(efficiencyZoneValue) * 100)
                                    .toFixed(2)
                                    .replace(".", ",") + "%"
                                : "--,--%",
                        backgroundColor: zoneBackgroundColor || "#f3f3f3",
                        fontColor: zoneFontColor || "#666666",
                    };
                }
            } else {
                console.log("⚠️ Списoк2.0 sheet not found");
                throw new Error(
                    `📋 Лист "Списoк2.0" не найден!\n\nОбратитесь к администратору для настройки системы.`
                );
            }
        } catch (e) {
            console.log("❌ Ошибка при получении данных из Списoк2.0:", e);
            // Если это уже наша пользовательская ошибка, перебрасываем как есть
            if (e.message && (e.message.includes("📋") || e.message.includes("🔒") || e.message.includes("📝"))) {
                throw e;
            }
            // Для всех остальных ошибок
            throw new Error(
                `📋 Ошибка доступа к базе данных!\n\nТехническая информация: ${e.message}\n\nОбратитесь к администратору.`
            );
        }

        const displayMaxCPL = maxCPLThreshold;
        const displayCPL_ROI_minus5 = maxCPLThreshold;

        // ПОСТРОЕНИЕ ЕДИНОГО SQL ЗАПРОСА
        let dateFilter = "";
        if (periodChosen) {
            if (hasStartDate && hasEndDate) {
                // Оба даты указаны
                const startDateStr = Utilities.formatDate(
                    new Date(periodStart),
                    "Europe/Kiev",
                    "yyyy-MM-dd"
                );
                const endDateStr = Utilities.formatDate(
                    new Date(periodEnd),
                    "Europe/Kiev",
                    "yyyy-MM-dd"
                );
                dateFilter = ` AND \`adv_date\` >= '${startDateStr}' AND \`adv_date\` <= '${endDateStr}'`;
                console.log("🔍 Date filter (both dates):", dateFilter);
            } else if (hasStartDate && !hasEndDate) {
                // Только начальная дата, до сегодня
                const startDateStr = Utilities.formatDate(
                    new Date(periodStart),
                    "Europe/Kiev",
                    "yyyy-MM-dd"
                );
                const todayStr = Utilities.formatDate(
                    new Date(),
                    "Europe/Kiev",
                    "yyyy-MM-dd"
                );
                dateFilter = ` AND \`adv_date\` >= '${startDateStr}' AND \`adv_date\` <= '${todayStr}'`;
                console.log("🔍 Date filter (start to today):", dateFilter);
            } else if (!hasStartDate && hasEndDate) {
                // Только конечная дата, с самого начала
                const endDateStr = Utilities.formatDate(
                    new Date(periodEnd),
                    "Europe/Kiev",
                    "yyyy-MM-dd"
                );
                dateFilter = ` AND \`adv_date\` <= '${endDateStr}'`;
                console.log("🔍 Date filter (beginning to end):", dateFilter);
            }
        } else {
            console.log("🔍 No date filter applied - showing all dates");
        }

        // ОБЪЕДИНЕННЫЙ SQL запрос для получения всех данных одним запросом
        const combinedSql = `
      SELECT 
        campaign_name,
        campaign_name_tracker,
        adv_group_name,
        adv_name,
        video_name,
        adv_date,
        adv_group_id,
        campaign_id,
        target_url,
        adv_id,
        cpc,
        cpm,
        clicks_on_link,
        ctr,
        frequency,
        average_time_on_video,
        valid_cpa,
        valid,
        cost,
        valid_cr,
        clicks_on_link_tracker,
        viewed_tracker,
        cpc_tracker,
        fraud,
        fraud_cpa,
        adv_group_budjet
      FROM \`ads_collection\`
      WHERE \`source\` = 'facebook' 
        AND (\`campaign_name\` LIKE '${article}%' OR \`campaign_name_tracker\` LIKE '${article}%')${dateFilter}
      ORDER BY adv_date
    `;

        // Получение данных из БД - ОДНИМ ЗАПРОСОМ
        console.log("🔍 Fetching all data with combined query...");
        console.log("🔍 SQL запрос:", combinedSql);
        console.log("🔍 Article:", article);
        console.log("🔍 Date filter applied:", periodChosen ? "YES" : "NO");
        if (periodChosen) {
            console.log("🔍 Filter params - Start:", periodStart, "End:", periodEnd);
        }
        let allData;

        try {
            console.log("🔍 Вызываем getDataFromDatabase...");
            allData = getDataFromDatabase(combinedSql);
            console.log(
                "🔍 Результат getDataFromDatabase:",
                allData ? allData.length : "null/undefined",
                "записей"
            );

            if (!allData || allData.length === 0) {
                console.log("❌ Данные не найдены для артикула:", article);
                throw new Error(
                    `📊 Данные не найдены!\n\nАртикул "${article}" не найден в базе данных.\n\nВозможные причины:\n• Проверьте правильность написания артикула\n• Артикул еще не добавлен в систему\n• Нет активных кампаний за выбранный период`
                );
            }

            console.log("✅ Данные успешно получены:", allData.length, "записей");
        } catch (error) {
            console.log("❌ Ошибка при получении данных:", error);
            if (error.message.includes("📊")) {
                throw error; // Перебрасываем наши пользовательские ошибки как есть
            }
            throw new Error(
                `🔌 Ошибка подключения к базе данных!\n\nТехническая информация: ${error.message}\n\nПопробуйте:\n• Обновить страницу\n• Проверить подключение к интернету\n• Обратиться к администратору`
            );
        }

        // Парсинг данных
        console.log("📊 Parsing database results...");
        const allRows = parseDbResults(allData);

        console.log("📈 Total rows from database:", allRows.length);

        // СОЗДАЕМ МАППИНГ campaign_name_tracker -> Buyer INFO для связки с TikTok данными
        const campaignToBuyerMap = {}; // campaign_name -> buyer info
        const adGroupToBuyerMap = {}; // adv_group_id -> buyer info

        // Сначала проходим по данным трекера и создаем маппинг
        console.log("🗺️ Creating buyer mapping from tracker data...");
        allRows.forEach((row) => {
            const trackerName = String(row.campaign_name_tracker || "").trim();
            const campaignName = String(row.campaign_name || "").trim();
            const groupId = String(row.adv_group_id || "").trim();
            const groupName = String(row.adv_group_name || "").trim();

            if (trackerName && trackerName.includes(article)) {
                const campaignInfo = parseCampaignName(trackerName);
                if (campaignInfo.buyer) {
                    // Создаем маппинг от campaign_name к buyer info
                    if (campaignName) {
                        campaignToBuyerMap[campaignName] = campaignInfo;
                        console.log(
                            `🔗 Mapped campaign "${campaignName}" to buyer "${campaignInfo.buyer}"`
                        );
                    }

                    // Создаем маппинг от adv_group_id к buyer info (с названием группы)
                    if (groupId && groupName) {
                        adGroupToBuyerMap[groupId] = {
                            ...campaignInfo,
                            groupName: groupName,
                        };
                        console.log(
                            `🔗 Mapped group_id "${groupId}" (${groupName}) to buyer "${campaignInfo.buyer}"`
                        );
                    }
                }
            }
        });

        console.log("🗺️ Created mappings:");
        console.log(
            "Campaign to buyer map:",
            Object.keys(campaignToBuyerMap).length,
            "entries"
        );
        console.log(
            "Group ID to buyer map:",
            Object.keys(adGroupToBuyerMap).length,
            "entries"
        );

        // СОЗДАЕМ СТРУКТУРЫ ДАННЫХ ДЛЯ ГРУППИРОВКИ
        console.log("🗂️ Processing data structures...");
        let minDate = null,
            maxDate = null;

        // Общие структуры
        const resultMap = {};
        const fbDataMap = {};

        // По байерам
        const resultMapByBuyer = {};
        const fbDataMapByBuyer = {};

        // По группам объявлений
        const resultMapByGroup = {};
        const fbDataMapByGroup = {};

        // НОВАЯ СТРУКТУРА: Байер → Кампания
        const resultMapByBuyerCampaign = {};
        const fbDataMapByBuyerCampaign = {};

        // НОВАЯ СТРУКТУРА: Байер → Кампания → Группа
        const resultMapByBuyerCampaignGroup = {};
        const fbDataMapByBuyerCampaignGroup = {};

        // НОВАЯ СТРУКТУРА: Байер → Группа → Объявление
        const resultMapByBuyerGroupAd = {};
        const fbDataMapByBuyerGroupAd = {};

        // Вспомогательные структуры
        const groupsByDate = {};
        const buyersByDate = {};
        const accountsByDate = {};
        const globalGroups = new Set();
        const globalBuyers = new Set();
        const globalAccounts = new Set();
        const buyerGroupsMap = {}; // { buyer: Set(groups) }
        let buyerVideosMap = {}; // { buyer: Set(videos) }
        let buyerSitesMap = {}; // { buyer: Set(sites) }
        let totalLeadsAll = 0,
            totalClicksAll = 0;
        const globalVideos = new Set(),
            globalSites = new Set();

        // Создаем базовую структуру Facebook метрик
        function createFacebookMetricsObject() {
            return {
                adId: [],
                freq: [],
                ctr: [],
                cpm: [],
                linkClicks: [],
                cpc: [],
                avgWatchTime: [],
                videoName: [],
                siteUrl: [],
                budget: [],
            };
        }

        // ОБРАБОТКА ВСЕХ ДАННЫХ СРАЗУ - используя маппинг для связки
        console.log("💰📘 Processing all data with buyer mapping...");
        allRows.forEach((row) => {
            const trackerName = String(row.campaign_name_tracker || "").trim();
            const campaignName = String(row.campaign_name || "").trim();
            const groupId = String(row.adv_group_id || "").trim();
            const groupName = String(row.adv_group_name || "").trim();
            const advName = String(row.adv_name || "").trim();
            const videoName = String(row.video_name || "").trim();
            const targetUrl = String(row.target_url || "").trim();
            const dateObj = new Date(row.adv_date);

            if (isNaN(dateObj.getTime())) return;

            const dateStr = Utilities.formatDate(
                dateObj,
                "Europe/Kiev",
                "yyyy-MM-dd"
            );

            // Определяем buyer info - ПРИОРИТЕТ tracker данным
            let buyerInfo = null;
            if (trackerName && trackerName.includes(article)) {
                buyerInfo = parseCampaignName(trackerName);
            } else if (campaignName && campaignToBuyerMap[campaignName]) {
                buyerInfo = campaignToBuyerMap[campaignName];
            } else if (groupId && adGroupToBuyerMap[groupId]) {
                buyerInfo = adGroupToBuyerMap[groupId];
            }

            if (!buyerInfo || buyerInfo.article !== article) return;

            // ДАННЫЕ ЛИДОВ И РАСХОДОВ (из tracker)
            const leads = Number(row.valid) || 0;
            const spend = Number(row.cost) || 0;
            const siteClicks = Number(row.clicks_on_link_tracker) || 0;

            // ДАННЫЕ FACEBOOK МЕТРИК (из Facebook)
            const hasMetrics = campaignName || groupId; // Есть ли Facebook метрики

            if (leads > 0 || spend > 0) {
                console.log(
                    `💰 Processing leads/spend for buyer: ${buyerInfo.buyer}, group: ${groupName}, date: ${dateStr}, leads: ${leads}, spend: ${spend}`
                );

                // Общие данные
                if (!resultMap[dateStr]) resultMap[dateStr] = { leads: 0, spend: 0 };
                resultMap[dateStr].leads += leads;
                resultMap[dateStr].spend += spend;

                // По байерам
                if (buyerInfo.buyer) {
                    if (!resultMapByBuyer[buyerInfo.buyer])
                        resultMapByBuyer[buyerInfo.buyer] = {};
                    if (!resultMapByBuyer[buyerInfo.buyer][dateStr])
                        resultMapByBuyer[buyerInfo.buyer][dateStr] = { leads: 0, spend: 0 };
                    resultMapByBuyer[buyerInfo.buyer][dateStr].leads += leads;
                    resultMapByBuyer[buyerInfo.buyer][dateStr].spend += spend;
                    globalBuyers.add(buyerInfo.buyer);

                    if (!buyersByDate[dateStr]) buyersByDate[dateStr] = [];
                    buyersByDate[dateStr].push(buyerInfo.buyer);
                }

                // По группам объявлений
                if (groupName) {
                    if (!resultMapByGroup[groupName]) resultMapByGroup[groupName] = {};
                    if (!resultMapByGroup[groupName][dateStr])
                        resultMapByGroup[groupName][dateStr] = { leads: 0, spend: 0 };
                    resultMapByGroup[groupName][dateStr].leads += leads;
                    resultMapByGroup[groupName][dateStr].spend += spend;
                    globalGroups.add(groupName);

                    if (!groupsByDate[dateStr]) groupsByDate[dateStr] = [];
                    groupsByDate[dateStr].push(groupName);
                }

                // Байер → Кампания
                if (buyerInfo.buyer && campaignName) {
                    const buyerCampaignKey = `${buyerInfo.buyer}:::${campaignName}`;
                    if (!resultMapByBuyerCampaign[buyerCampaignKey])
                        resultMapByBuyerCampaign[buyerCampaignKey] = {};
                    if (!resultMapByBuyerCampaign[buyerCampaignKey][dateStr])
                        resultMapByBuyerCampaign[buyerCampaignKey][dateStr] = {
                            leads: 0,
                            spend: 0,
                        };
                    resultMapByBuyerCampaign[buyerCampaignKey][dateStr].leads += leads;
                    resultMapByBuyerCampaign[buyerCampaignKey][dateStr].spend += spend;
                }

                // Байер → Кампания → Группа
                if (buyerInfo.buyer && campaignName && groupName) {
                    const buyerCampaignGroupKey = `${buyerInfo.buyer}:::${campaignName}:::${groupName}`;
                    if (!resultMapByBuyerCampaignGroup[buyerCampaignGroupKey])
                        resultMapByBuyerCampaignGroup[buyerCampaignGroupKey] = {};
                    if (!resultMapByBuyerCampaignGroup[buyerCampaignGroupKey][dateStr])
                        resultMapByBuyerCampaignGroup[buyerCampaignGroupKey][dateStr] = {
                            leads: 0,
                            spend: 0,
                        };
                    resultMapByBuyerCampaignGroup[buyerCampaignGroupKey][dateStr].leads += leads;
                    resultMapByBuyerCampaignGroup[buyerCampaignGroupKey][dateStr].spend += spend;

                    // Отслеживаем группы для каждого байера
                    if (!buyerGroupsMap[buyerInfo.buyer])
                        buyerGroupsMap[buyerInfo.buyer] = new Set();
                    buyerGroupsMap[buyerInfo.buyer].add(groupName);
                }

                // Байер → Группа → Объявление
                if (buyerInfo.buyer && groupName && advName) {
                    const buyerGroupAdKey = `${buyerInfo.buyer}:::${groupName}:::${advName}`;
                    if (!resultMapByBuyerGroupAd[buyerGroupAdKey])
                        resultMapByBuyerGroupAd[buyerGroupAdKey] = {};
                    if (!resultMapByBuyerGroupAd[buyerGroupAdKey][dateStr])
                        resultMapByBuyerGroupAd[buyerGroupAdKey][dateStr] = {
                            leads: 0,
                            spend: 0,
                        };
                    resultMapByBuyerGroupAd[buyerGroupAdKey][dateStr].leads += leads;
                    resultMapByBuyerGroupAd[buyerGroupAdKey][dateStr].spend += spend;
                }

                // Учитываем день для CR только если есть данные о кликах
                if (hasMetrics && siteClicks > 0) {
                    totalLeadsAll += leads;
                    totalClicksAll += siteClicks;
                }

                if (!minDate || dateObj < minDate) minDate = dateObj;
                if (!maxDate || dateObj > maxDate) maxDate = dateObj;
            }

            // FACEBOOK МЕТРИКИ (frequency, CTR, CPM, etc.)
            if (hasMetrics && (campaignName || groupId)) {
                console.log(
                    `📘 Processing Facebook metrics for buyer: ${buyerInfo.buyer}, group: ${groupName}, date: ${dateStr}`
                );

                // Добавляем метрики в структуру
                function addFacebookMetrics(targetObject, dateKey) {
                    if (!targetObject[dateKey]) {
                        targetObject[dateKey] = createFacebookMetricsObject();
                    }

                    targetObject[dateKey].adId.push(
                        row.adv_id !== undefined && row.adv_id !== null
                            ? String(row.adv_id)
                            : ""
                    );
                    targetObject[dateKey].freq.push(
                        row.frequency !== undefined && row.frequency !== null
                            ? String(row.frequency)
                            : ""
                    );
                    targetObject[dateKey].ctr.push(
                        row.ctr !== undefined && row.ctr !== null ? String(row.ctr) : ""
                    );
                    targetObject[dateKey].cpm.push(
                        row.cpm !== undefined && row.cpm !== null ? String(row.cpm) : ""
                    );
                    targetObject[dateKey].linkClicks.push(
                        row.clicks_on_link_tracker !== undefined && row.clicks_on_link_tracker !== null
                            ? String(row.clicks_on_link_tracker)
                            : ""
                    );
                    targetObject[dateKey].cpc.push(
                        row.cpc !== undefined && row.cpc !== null ? String(row.cpc) : ""
                    );
                    targetObject[dateKey].avgWatchTime.push(
                        row.average_time_on_video !== undefined &&
                            row.average_time_on_video !== null
                            ? String(row.average_time_on_video)
                            : ""
                    );
                    targetObject[dateKey].videoName.push(videoName || "");
                    targetObject[dateKey].siteUrl.push(targetUrl || "");
                    const budgetData =
                        row.adv_group_budjet !== undefined && row.adv_group_budjet !== null
                            ? String(row.adv_group_budjet)
                            : "";
                    targetObject[dateKey].budget.push(budgetData);
                    console.log(
                        "🔍 Added budget to metrics:",
                        budgetData,
                        "for date:",
                        dateKey,
                        "raw value:",
                        row.adv_group_budjet
                    );
                }

                // ОБЩИЕ ДАННЫЕ
                addFacebookMetrics(fbDataMap, dateStr);

                // ПО БАЙЕРАМ
                if (buyerInfo.buyer) {
                    if (!fbDataMapByBuyer[buyerInfo.buyer])
                        fbDataMapByBuyer[buyerInfo.buyer] = {};
                    addFacebookMetrics(fbDataMapByBuyer[buyerInfo.buyer], dateStr);
                }

                // ПО ГРУППАМ ОБЪЯВЛЕНИЙ
                if (groupName) {
                    if (!fbDataMapByGroup[groupName]) fbDataMapByGroup[groupName] = {};
                    addFacebookMetrics(fbDataMapByGroup[groupName], dateStr);
                }

                // БАЙЕР → КАМПАНИЯ
                if (buyerInfo.buyer && campaignName) {
                    const buyerCampaignKey = `${buyerInfo.buyer}:::${campaignName}`;
                    if (!fbDataMapByBuyerCampaign[buyerCampaignKey])
                        fbDataMapByBuyerCampaign[buyerCampaignKey] = {};
                    addFacebookMetrics(fbDataMapByBuyerCampaign[buyerCampaignKey], dateStr);
                }

                // БАЙЕР → КАМПАНИЯ → ГРУППА
                if (buyerInfo.buyer && campaignName && groupName) {
                    const buyerCampaignGroupKey = `${buyerInfo.buyer}:::${campaignName}:::${groupName}`;
                    if (!fbDataMapByBuyerCampaignGroup[buyerCampaignGroupKey])
                        fbDataMapByBuyerCampaignGroup[buyerCampaignGroupKey] = {};
                    addFacebookMetrics(fbDataMapByBuyerCampaignGroup[buyerCampaignGroupKey], dateStr);
                }

                // БАЙЕР → ГРУППА → ОБЪЯВЛЕНИЕ
                if (buyerInfo.buyer && groupName && advName) {
                    const buyerGroupAdKey = `${buyerInfo.buyer}:::${groupName}:::${advName}`;
                    if (!fbDataMapByBuyerGroupAd[buyerGroupAdKey])
                        fbDataMapByBuyerGroupAd[buyerGroupAdKey] = {};
                    addFacebookMetrics(fbDataMapByBuyerGroupAd[buyerGroupAdKey], dateStr);
                }

                // Собираем уникальные видео и сайты с привязкой к байерам
                if (videoName && videoName.trim() !== "") {
                    globalVideos.add(videoName.trim());
                    // Привязываем видео к байеру
                    if (!buyerVideosMap[buyerInfo.buyer])
                        buyerVideosMap[buyerInfo.buyer] = new Set();
                    buyerVideosMap[buyerInfo.buyer].add(videoName.trim());
                }
                if (targetUrl && targetUrl.trim() !== "") {
                    globalSites.add(targetUrl.trim());
                    // Привязываем сайт к байеру
                    if (!buyerSitesMap[buyerInfo.buyer])
                        buyerSitesMap[buyerInfo.buyer] = new Set();
                    buyerSitesMap[buyerInfo.buyer].add(targetUrl.trim());
                }
            }

            // По аккаунтам
            if (buyerInfo.account) {
                globalAccounts.add(buyerInfo.account);
                if (!accountsByDate[dateStr]) accountsByDate[dateStr] = [];
                accountsByDate[dateStr].push(buyerInfo.account);
            }
        });

        console.log("📊 Data processing completed!");
        console.log("👥 Found buyers:", Array.from(globalBuyers));
        console.log("📁 Found groups:", Array.from(globalGroups));
        console.log("🎬 Found videos:", globalVideos.size);
        console.log("🌐 Found sites:", globalSites.size);
        console.log("🗂️ Buyer groups mapping:", buyerGroupsMap);

        if (!minDate) {
            throw new Error(
                `📊 Нет активных данных!\n\nПо артикулу "${article}" не найдено активных периодов.\n\nУбедитесь, что:\n• Артикул написан правильно\n• Кампании имели расходы\n• Выбран корректный период`
            );
        }

        if (periodChosen) {
            if (hasStartDate) {
                minDate = new Date(periodStart);
            }
            if (hasEndDate) {
                maxDate = new Date(periodEnd);
            }
        }

        // Массив дат - только активные даты
        let firstActiveDate = null,
            lastActiveDate = null;

        // Находим первую и последнюю активную дату из данных
        Object.keys(resultMap).forEach((dateKey) => {
            const rec = resultMap[dateKey];
            if (rec.spend > 0) {
                const dateObj = new Date(dateKey);
                if (!firstActiveDate || dateObj < firstActiveDate)
                    firstActiveDate = dateObj;
                if (!lastActiveDate || dateObj > lastActiveDate)
                    lastActiveDate = dateObj;
            }
        });

        // Если период выбран, ограничиваем датами периода
        if (periodChosen) {
            if (
                hasStartDate &&
                minDate &&
                (!firstActiveDate || minDate > firstActiveDate)
            ) {
                firstActiveDate = minDate;
            }
            if (
                hasEndDate &&
                maxDate &&
                (!lastActiveDate || maxDate < lastActiveDate)
            ) {
                lastActiveDate = maxDate;
            }
        }

        const allDates = [];
        if (firstActiveDate && lastActiveDate) {
            curDate = new Date(firstActiveDate);
            while (curDate <= lastActiveDate) {
                allDates.push(new Date(curDate));
                curDate.setDate(curDate.getDate() + 1);
            }
        }

        // Функция для обработки сегмента
        function processSegment(
            segmentName,
            resultMapBySegment,
            fbDataMapBySegment,
            segmentType
        ) {
            console.log(`🔄 Processing segment: ${segmentName} (${segmentType})`);
            console.log(`🔍 Segment data check for ${segmentName}:`, {
                hasResultMap: !!resultMapBySegment[segmentName],
                resultMapKeys: resultMapBySegment[segmentName] ? Object.keys(resultMapBySegment[segmentName]).length : 0,
                hasFbDataMap: !!fbDataMapBySegment[segmentName],
                fbDataMapKeys: fbDataMapBySegment[segmentName] ? Object.keys(fbDataMapBySegment[segmentName]).length : 0
            });

            let segmentMinDate = null,
                segmentMaxDate = null;

            // Используем только даты из данных сегмента
            const segmentDateKeys = Object.keys(
                resultMapBySegment[segmentName] || {}
            );
            let checkDate =
                segmentDateKeys.length > 0
                    ? new Date(Math.min(...segmentDateKeys.map((d) => new Date(d))))
                    : new Date();
            const endDate =
                segmentDateKeys.length > 0
                    ? new Date(Math.max(...segmentDateKeys.map((d) => new Date(d))))
                    : new Date();

            while (checkDate <= endDate) {
                const dateKey = Utilities.formatDate(
                    checkDate,
                    "Europe/Kiev",
                    "yyyy-MM-dd"
                );
                const rec = resultMapBySegment[segmentName]
                    ? resultMapBySegment[segmentName][dateKey] || { leads: 0, spend: 0 }
                    : { leads: 0, spend: 0 };

                if (rec.spend > 0) {
                    if (!segmentMinDate) segmentMinDate = new Date(checkDate);
                    segmentMaxDate = new Date(checkDate);
                }

                checkDate.setDate(checkDate.getDate() + 1);
            }

            if (!segmentMinDate || !segmentMaxDate) {
                console.log(`⚠️ No active data found for segment: ${segmentName}`);
                return null;
            }

            console.log(
                `✅ Segment ${segmentName} has data from ${segmentMinDate.toISOString().split("T")[0]
                } to ${segmentMaxDate.toISOString().split("T")[0]}`
            );

            const segmentDates = [];
            let curDateSeg = new Date(segmentMinDate);
            while (curDateSeg <= segmentMaxDate) {
                segmentDates.push(new Date(curDateSeg));
                curDateSeg.setDate(curDateSeg.getDate() + 1);
            }

            const segmentData = {
                dates: [],
                ratings: [],
                cplDay: [],
                leadsDay: [],
                spendDay: [],
                conversionDay: [],
                maxCPL: [],
                cplCumulative: [],
                cplCumulativeColors: [],
                cplCumulativeArrows: [],
                groups: [],
                freq: [],
                ctr: [],
                cpm: [],
                linkClicks: [],
                cpc: [],
                avgWatchTime: [],
                videoName: [],
                siteUrl: [],
                budget: [],
            };

            let activeDaysSegment = 0,
                daysInNormSegment = 0,
                daysBelowAllowedSegment = 0;
            let segmentLeads = 0,
                segmentClicks = 0;
            const segmentVideos = new Set(),
                segmentSites = new Set();
            let aggCostSegment = 0,
                aggLeadsSegment = 0,
                prevDayGoodSegment = null;

            for (let i = 0; i < segmentDates.length; i++) {
                const d = segmentDates[i];
                const dateKey = Utilities.formatDate(d, "Europe/Kiev", "yyyy-MM-dd");
                const dateDisplay = Utilities.formatDate(
                    d,
                    "Europe/Kiev",
                    "dd.MM.yyyy"
                );

                segmentData.dates.push(dateDisplay);

                const rec = resultMapBySegment[segmentName]
                    ? resultMapBySegment[segmentName][dateKey] || { leads: 0, spend: 0 }
                    : { leads: 0, spend: 0 };
                const dayLeads = rec.leads;
                const daySpend = rec.spend;
                const dayCpl = dayLeads > 0 ? daySpend / dayLeads : 0;

                const fbDataSegment =
                    dayLeads > 0 || daySpend > 0
                        ? (fbDataMapBySegment[segmentName] &&
                            fbDataMapBySegment[segmentName][dateKey]) ||
                        createFacebookMetricsObject()
                        : createFacebookMetricsObject();

                if (dayLeads === 0 && daySpend === 0) {
                    segmentData.cplDay.push(0);
                    segmentData.leadsDay.push(0);
                    segmentData.spendDay.push(0);
                    segmentData.conversionDay.push("0.00%");
                    segmentData.maxCPL.push(displayMaxCPL);
                    segmentData.ratings.push("");

                    // Группы для байера (даже для нулевых дней)
                    if (segmentType === "buyer") {
                        segmentData.groups.push("");
                    } else {
                        segmentData.groups.push("");
                    }

                    segmentData.freq.push("");
                    segmentData.ctr.push("");
                    segmentData.cpm.push("");
                    segmentData.linkClicks.push("");
                    segmentData.cpc.push("");
                    segmentData.avgWatchTime.push("");
                    segmentData.videoName.push("");
                    segmentData.siteUrl.push("");
                    segmentData.budget.push("");

                    aggCostSegment = 0;
                    aggLeadsSegment = 0;
                    segmentData.cplCumulative.push(0);
                    segmentData.cplCumulativeColors.push("gray");
                    segmentData.cplCumulativeArrows.push("");
                    prevDayGoodSegment = null;
                    continue;
                }

                let segmentDayConversionText = "--";
                if (fbDataSegment.linkClicks && dayLeads > 0) {
                    const segmentDayClicks = sumMultilineValues(fbDataSegment.linkClicks);
                    if (segmentDayClicks > 0) {
                        const segmentDayConversion = (dayLeads / segmentDayClicks) * 100;
                        segmentDayConversionText = segmentDayConversion.toFixed(2) + "%";
                    }
                }

                segmentData.cplDay.push(dayCpl);
                segmentData.leadsDay.push(dayLeads);
                segmentData.spendDay.push(daySpend);
                segmentData.conversionDay.push(segmentDayConversionText);
                segmentData.maxCPL.push(displayMaxCPL);

                // Группы для байера
                if (segmentType === "buyer") {
                    const dayGroupsForBuyer = groupsByDate[dateKey]
                        ? groupsByDate[dateKey].filter((group) => {
                            const buyerGroups = buyerGroupsMap[segmentName] || new Set();
                            return buyerGroups.has(group);
                        })
                        : [];
                    const uniqueGroupsForBuyer = Array.from(
                        new Set(
                            dayGroupsForBuyer.filter(
                                (g) => g !== undefined && g !== null && g !== ""
                            )
                        )
                    ).reverse();
                    segmentData.groups.push(uniqueGroupsForBuyer.join("\n"));
                } else {
                    segmentData.groups.push("");
                }

                if (dayLeads > 0 || daySpend > 0) activeDaysSegment++;

                segmentData.freq.push(processDayValues(fbDataSegment.freq, 11));
                segmentData.ctr.push(processDayValues(fbDataSegment.ctr, 12));
                segmentData.cpm.push(processDayValues(fbDataSegment.cpm, 13));
                segmentData.linkClicks.push(
                    processDayValues(fbDataSegment.linkClicks, 14)
                );
                segmentData.cpc.push(processDayValues(fbDataSegment.cpc, 15));
                segmentData.avgWatchTime.push(
                    processDayValues(fbDataSegment.avgWatchTime, 16)
                );
                segmentData.videoName.push(
                    processDayValues(fbDataSegment.videoName, 17)
                );
                segmentData.siteUrl.push(processDayValues(fbDataSegment.siteUrl, 18));
                const budgetValue = processDayValues(fbDataSegment.budget || [], 19);
                segmentData.budget.push(budgetValue);
                console.log(
                    `🔍 Segment ${segmentName} - Added budget for day ${dateDisplay}:`,
                    budgetValue,
                    "Raw budget data:",
                    fbDataSegment.budget
                );

                if (dayLeads > 0 && dayCpl <= displayMaxCPL) {
                    daysInNormSegment++;
                } else if (daySpend > 0) {
                    daysBelowAllowedSegment++;
                }

                fbDataSegment.videoName?.forEach((video) => {
                    if (video && video.trim() !== "") {
                        segmentVideos.add(video.trim());
                    }
                });
                fbDataSegment.siteUrl?.forEach((site) => {
                    if (site && site.trim() !== "") {
                        segmentSites.add(site.trim());
                    }
                });

                const dayClicksForCR = sumMultilineValues(fbDataSegment.linkClicks || []);
                if (dayClicksForCR > 0) {
                    segmentLeads += dayLeads;
                    segmentClicks += dayClicksForCR;
                }

                let rating;
                if (dayLeads === 0 && daySpend > 0) {
                    rating = "D";
                } else {
                    rating = calculateRating(dayCpl, maxCPLThreshold);
                }
                segmentData.ratings.push(rating);

                const dayIsGood = dayLeads > 0 && dayCpl <= displayMaxCPL;

                let arrow = "";
                if (prevDayGoodSegment !== null && prevDayGoodSegment !== dayIsGood) {
                    if (dayIsGood) {
                        arrow = "↗";
                    } else {
                        arrow = "↘";
                    }
                }

                if (prevDayGoodSegment !== null && prevDayGoodSegment !== dayIsGood) {
                    aggCostSegment = daySpend;
                    aggLeadsSegment = dayLeads;
                } else {
                    aggCostSegment += daySpend;
                    aggLeadsSegment += dayLeads;
                }

                const finalCpl =
                    aggLeadsSegment > 0 ? aggCostSegment / aggLeadsSegment : 0;
                segmentData.cplCumulative.push(finalCpl);

                segmentData.cplCumulativeColors.push(dayIsGood ? "green" : "red");
                segmentData.cplCumulativeArrows.push(arrow);

                prevDayGoodSegment = dayIsGood;
            }

            const segmentCR =
                segmentClicks > 0 ? (segmentLeads / segmentClicks) * 100 : 0;

            // Группируем даты в диапазоны для сегмента
            const segmentDateRanges = groupDateRanges(
                segmentData.dates,
                segmentData.spendDay
            );

            // Создаем новые массивы с диапазонами для сегмента
            const newSegmentData = {
                dates: [],
                ratings: [],
                cplDay: [],
                leadsDay: [],
                spendDay: [],
                conversionDay: [],
                maxCPL: [],
                cplCumulative: [],
                cplCumulativeColors: [],
                cplCumulativeArrows: [],
                groups: [],
                freq: [],
                ctr: [],
                cpm: [],
                linkClicks: [],
                cpc: [],
                avgWatchTime: [],
                videoName: [],
                siteUrl: [],
                budget: [],
                columnSpans: [],
                columnClasses: [],
            };

            segmentDateRanges.forEach((range) => {
                if (range.isZeroRange && range.startIndex !== range.endIndex) {
                    const rangeLabel = formatDateRange(range.startDate, range.endDate);
                    newSegmentData.dates.push(rangeLabel);
                    newSegmentData.columnSpans.push(
                        range.endIndex - range.startIndex + 1
                    );
                    newSegmentData.columnClasses.push("zero-spend-range");

                    newSegmentData.ratings.push("");
                    newSegmentData.cplDay.push(0);
                    newSegmentData.leadsDay.push(0);
                    newSegmentData.spendDay.push(0);
                    newSegmentData.conversionDay.push("0.00%");
                    newSegmentData.maxCPL.push(segmentData.maxCPL[range.startIndex] || 0);
                    newSegmentData.cplCumulative.push(0);
                    newSegmentData.cplCumulativeColors.push("gray");
                    newSegmentData.cplCumulativeArrows.push("");
                    newSegmentData.groups.push("");
                    newSegmentData.freq.push("");
                    newSegmentData.ctr.push("");
                    newSegmentData.cpm.push("");
                    newSegmentData.linkClicks.push("");
                    newSegmentData.cpc.push("");
                    newSegmentData.avgWatchTime.push("");
                    newSegmentData.videoName.push("");
                    newSegmentData.siteUrl.push("");
                    newSegmentData.budget.push("");
                } else {
                    for (let i = range.startIndex; i <= range.endIndex; i++) {
                        newSegmentData.dates.push(segmentData.dates[i]);
                        newSegmentData.columnSpans.push(1);
                        newSegmentData.columnClasses.push(
                            segmentData.spendDay[i] === 0
                                ? "zero-spend-single"
                                : "normal-spend"
                        );

                        newSegmentData.ratings.push(segmentData.ratings[i]);
                        newSegmentData.cplDay.push(segmentData.cplDay[i]);
                        newSegmentData.leadsDay.push(segmentData.leadsDay[i]);
                        newSegmentData.spendDay.push(segmentData.spendDay[i]);
                        newSegmentData.conversionDay.push(segmentData.conversionDay[i]);
                        newSegmentData.maxCPL.push(segmentData.maxCPL[i]);
                        newSegmentData.cplCumulative.push(segmentData.cplCumulative[i]);
                        newSegmentData.cplCumulativeColors.push(
                            segmentData.cplCumulativeColors[i]
                        );
                        newSegmentData.cplCumulativeArrows.push(
                            segmentData.cplCumulativeArrows[i]
                        );
                        newSegmentData.groups.push(segmentData.groups[i]);
                        newSegmentData.freq.push(segmentData.freq[i]);
                        newSegmentData.ctr.push(segmentData.ctr[i]);
                        newSegmentData.cpm.push(segmentData.cpm[i]);
                        newSegmentData.linkClicks.push(segmentData.linkClicks[i]);
                        newSegmentData.cpc.push(segmentData.cpc[i]);
                        newSegmentData.avgWatchTime.push(segmentData.avgWatchTime[i]);
                        newSegmentData.videoName.push(segmentData.videoName[i]);
                        newSegmentData.siteUrl.push(segmentData.siteUrl[i]);
                        newSegmentData.budget.push(segmentData.budget[i]);
                    }
                }
            });

            Object.assign(segmentData, newSegmentData);

            console.log(
                `✅ Processed segment ${segmentName}: ${activeDaysSegment} active days, ${segmentVideos.size} videos, ${segmentSites.size} sites`
            );
            console.log(
                `🔍 Segment ${segmentName} budget data sample:`,
                segmentData.budget ? segmentData.budget.slice(0, 3) : "no budget data"
            );

            return {
                data: segmentData,
                metrics: {
                    activeDays: activeDaysSegment,
                    daysInNorm: daysInNormSegment,
                    daysBelowAllowed: daysBelowAllowedSegment,
                    cr: segmentCR.toFixed(2).replace(".", ",") + "%",
                    videos: segmentVideos.size,
                    sites: segmentSites.size,
                    videoNames: Array.from(segmentVideos).join('\n') || 'Нет данных',
                    siteUrls: Array.from(segmentSites).join('\n') || 'Нет данных',
                },
            };
        }

        // Функция для группировки дат в диапазоны
        function groupDateRanges(dates, spends) {
            const ranges = [];
            let currentRange = null;

            for (let i = 0; i < dates.length; i++) {
                const isZeroSpend = spends[i] === 0;

                if (isZeroSpend) {
                    if (!currentRange) {
                        currentRange = {
                            startIndex: i,
                            endIndex: i,
                            startDate: dates[i],
                            endDate: dates[i],
                            isZeroRange: true,
                        };
                    } else {
                        currentRange.endIndex = i;
                        currentRange.endDate = dates[i];
                    }
                } else {
                    if (currentRange) {
                        ranges.push(currentRange);
                        currentRange = null;
                    }
                    ranges.push({
                        startIndex: i,
                        endIndex: i,
                        startDate: dates[i],
                        endDate: dates[i],
                        isZeroRange: false,
                    });
                }
            }

            if (currentRange) {
                ranges.push(currentRange);
            }

            return ranges;
        }

        // Функция для форматирования диапазона дат
        function formatDateRange(startDate, endDate) {
            if (startDate === endDate) {
                return startDate;
            }

            // Вычисляем количество дней в диапазоне
            const start = new Date(startDate.split('.').reverse().join('-'));
            const end = new Date(endDate.split('.').reverse().join('-'));
            const daysDiff = Math.round((end - start) / (1000 * 60 * 60 * 24)) + 1;

            return `${daysDiff} д.`;
        }

        // Подготовка данных для общей таблицы
        console.log("📊 Building general data...");
        const generalData = {
            dates: [],
            ratings: [],
            cplDay: [],
            leadsDay: [],
            spendDay: [],
            conversionDay: [],
            maxCPL: [],
            cplCumulative: [],
            cplCumulativeColors: [],
            cplCumulativeArrows: [],
            groups: [],
            buyers: [],
            accounts: [],
            freq: [],
            ctr: [],
            cpm: [],
            linkClicks: [],
            cpc: [],
            avgWatchTime: [],
            videoName: [],
            siteUrl: [],
            budget: [],
            columnSpans: [],
            columnClasses: [],
        };
        let activeDays = 0,
            daysInNorm = 0,
            daysBelowAllowed = 0;
        let aggCost = 0,
            aggLeads = 0,
            prevDayGood = null;

        // Заполнение общих данных
        for (let i = 0; i < allDates.length; i++) {
            const d = allDates[i];
            const dateKey = Utilities.formatDate(d, "Europe/Kiev", "yyyy-MM-dd");
            const dateDisplay = Utilities.formatDate(d, "Europe/Kiev", "dd.MM.yyyy");

            generalData.dates.push(dateDisplay);

            const rec = resultMap[dateKey] || { leads: 0, spend: 0 };
            const dayLeads = rec.leads;
            const daySpend = rec.spend;

            if (dayLeads === 0 && daySpend === 0) {
                generalData.cplDay.push(0);
                generalData.leadsDay.push(0);
                generalData.spendDay.push(0);
                generalData.conversionDay.push("0.00%");
                generalData.maxCPL.push(displayMaxCPL);
                generalData.groups.push("");
                generalData.buyers.push("");
                generalData.accounts.push("");
                generalData.ratings.push("");

                generalData.freq.push("");
                generalData.ctr.push("");
                generalData.cpm.push("");
                generalData.linkClicks.push("");
                generalData.cpc.push("");
                generalData.avgWatchTime.push("");
                generalData.videoName.push("");
                generalData.siteUrl.push("");
                generalData.budget.push("");
                console.log("🔍 Added empty budget for zero day");

                aggCost = 0;
                aggLeads = 0;
                generalData.cplCumulative.push(0);
                generalData.cplCumulativeColors.push("gray");
                generalData.cplCumulativeArrows.push("");
                prevDayGood = null;
                continue;
            }

            const dayCpl = dayLeads > 0 ? daySpend / dayLeads : 0;

            let dayConversionText = "--";
            if (fbDataMap[dateKey] && fbDataMap[dateKey].linkClicks && dayLeads > 0) {
                const dayClicks = sumMultilineValues(fbDataMap[dateKey].linkClicks);
                if (dayClicks > 0) {
                    const dayConversion = (dayLeads / dayClicks) * 100;
                    dayConversionText = dayConversion.toFixed(2) + "%";
                }
            }

            generalData.cplDay.push(dayCpl);
            generalData.leadsDay.push(dayLeads);
            generalData.spendDay.push(daySpend);
            generalData.conversionDay.push(dayConversionText);
            generalData.maxCPL.push(displayMaxCPL);

            // Получаем данные дня
            const dayGroups = groupsByDate[dateKey] || [];
            const dayBuyers = buyersByDate[dateKey] || [];
            const dayAccounts = accountsByDate[dateKey] || [];

            // Уникальные байеры с сохранением порядка появления
            const uniqueBuyers = [];
            dayBuyers.forEach((buyer) => {
                if (buyer && buyer.trim() !== "" && !uniqueBuyers.includes(buyer)) {
                    uniqueBuyers.push(buyer);
                }
            });

            // Группируем данные по байерам для согласованной сортировки
            const sortedGroupsByBuyer = [];
            const sortedAccountsByBuyer = [];

            uniqueBuyers.forEach((buyer) => {
                // Найдем все группы этого байера на этот день
                const buyerGroups = [];
                const buyerAccounts = [];

                dayGroups.forEach((group) => {
                    if (group && group.trim() !== "") {
                        // Проверяем принадлежность группы к байеру через buyerGroupsMap
                        if (buyerGroupsMap[buyer] && buyerGroupsMap[buyer].has(group)) {
                            if (!buyerGroups.includes(group)) {
                                buyerGroups.push(group);
                            }
                        }
                    }
                });

                dayAccounts.forEach((account) => {
                    if (
                        account &&
                        account.trim() !== "" &&
                        !buyerAccounts.includes(account)
                    ) {
                        buyerAccounts.push(account);
                    }
                });

                sortedGroupsByBuyer.push(...buyerGroups);
                sortedAccountsByBuyer.push(...buyerAccounts);
            });

            // Добавляем отсортированные данные
            generalData.groups.push(sortedGroupsByBuyer.join("\n"));
            generalData.buyers.push(uniqueBuyers.join("\n"));
            generalData.accounts.push(sortedAccountsByBuyer.join("\n"));

            if (dayLeads > 0 || daySpend > 0) activeDays++;

            if (dayLeads > 0 && dayCpl <= displayMaxCPL) {
                daysInNorm++;
            } else if (daySpend > 0) {
                daysBelowAllowed++;
            }

            generalData.freq.push(
                processDayValues(fbDataMap[dateKey] ? fbDataMap[dateKey].freq : [], 11)
            );
            generalData.ctr.push(
                processDayValues(fbDataMap[dateKey] ? fbDataMap[dateKey].ctr : [], 12)
            );
            generalData.cpm.push(
                processDayValues(fbDataMap[dateKey] ? fbDataMap[dateKey].cpm : [], 13)
            );
            generalData.linkClicks.push(
                processDayValues(
                    fbDataMap[dateKey] ? fbDataMap[dateKey].linkClicks : [],
                    14
                )
            );
            generalData.cpc.push(
                processDayValues(fbDataMap[dateKey] ? fbDataMap[dateKey].cpc : [], 15)
            );
            generalData.avgWatchTime.push(
                processDayValues(
                    fbDataMap[dateKey] ? fbDataMap[dateKey].avgWatchTime : [],
                    16
                )
            );
            generalData.videoName.push(
                processDayValues(
                    fbDataMap[dateKey] ? fbDataMap[dateKey].videoName : [],
                    17
                )
            );
            generalData.siteUrl.push(
                processDayValues(
                    fbDataMap[dateKey] ? fbDataMap[dateKey].siteUrl : [],
                    18
                )
            );
            const budgetValue = processDayValues(
                fbDataMap[dateKey] ? fbDataMap[dateKey].budget : [],
                19
            );
            generalData.budget.push(budgetValue);
            console.log(
                "🔍 Added budget for active day:",
                dateKey,
                "value:",
                budgetValue
            );

            let rating;
            if (dayLeads === 0 && daySpend > 0) {
                rating = "D";
            } else {
                rating = calculateRating(dayCpl, maxCPLThreshold);
            }
            generalData.ratings.push(rating);

            const dayIsGood = dayLeads > 0 && dayCpl <= displayMaxCPL;

            let arrow = "";
            if (prevDayGood !== null && prevDayGood !== dayIsGood) {
                if (dayIsGood) {
                    arrow = "↗";
                } else {
                    arrow = "↘";
                }
            }

            if (prevDayGood !== null && prevDayGood !== dayIsGood) {
                aggCost = daySpend;
                aggLeads = dayLeads;
            } else {
                aggCost += daySpend;
                aggLeads += dayLeads;
            }

            const finalCpl = aggLeads > 0 ? aggCost / aggLeads : 0;
            generalData.cplCumulative.push(finalCpl);

            generalData.cplCumulativeColors.push(dayIsGood ? "green" : "red");
            generalData.cplCumulativeArrows.push(arrow);

            prevDayGood = dayIsGood;
        }

        // Группируем даты в диапазоны
        const dateRanges = groupDateRanges(generalData.dates, generalData.spendDay);

        // Создаем новые массивы с диапазонами
        const newGeneralData = {
            dates: [],
            ratings: [],
            cplDay: [],
            leadsDay: [],
            spendDay: [],
            conversionDay: [],
            maxCPL: [],
            cplCumulative: [],
            cplCumulativeColors: [],
            cplCumulativeArrows: [],
            groups: [],
            buyers: [],
            accounts: [],
            freq: [],
            ctr: [],
            cpm: [],
            linkClicks: [],
            cpc: [],
            avgWatchTime: [],
            videoName: [],
            siteUrl: [],
            budget: [],
            columnSpans: [],
            columnClasses: [],
        };

        dateRanges.forEach((range) => {
            if (range.isZeroRange && range.startIndex !== range.endIndex) {
                const rangeLabel = formatDateRange(range.startDate, range.endDate);
                newGeneralData.dates.push(rangeLabel);
                newGeneralData.columnSpans.push(range.endIndex - range.startIndex + 1);
                newGeneralData.columnClasses.push("zero-spend-range");

                newGeneralData.ratings.push("");
                newGeneralData.cplDay.push(0);
                newGeneralData.leadsDay.push(0);
                newGeneralData.spendDay.push(0);
                newGeneralData.conversionDay.push("0.00%");
                newGeneralData.maxCPL.push(generalData.maxCPL[range.startIndex]);
                newGeneralData.cplCumulative.push(0);
                newGeneralData.cplCumulativeColors.push("gray");
                newGeneralData.cplCumulativeArrows.push("");
                newGeneralData.groups.push("");
                newGeneralData.buyers.push("");
                newGeneralData.accounts.push("");
                newGeneralData.freq.push("");
                newGeneralData.ctr.push("");
                newGeneralData.cpm.push("");
                newGeneralData.linkClicks.push("");
                newGeneralData.cpc.push("");
                newGeneralData.avgWatchTime.push("");
                newGeneralData.videoName.push("");
                newGeneralData.siteUrl.push("");
                newGeneralData.budget.push("");
            } else {
                for (let i = range.startIndex; i <= range.endIndex; i++) {
                    newGeneralData.dates.push(generalData.dates[i]);
                    newGeneralData.columnSpans.push(1);
                    newGeneralData.columnClasses.push(
                        generalData.spendDay[i] === 0 ? "zero-spend-single" : "normal-spend"
                    );

                    newGeneralData.ratings.push(generalData.ratings[i]);
                    newGeneralData.cplDay.push(generalData.cplDay[i]);
                    newGeneralData.leadsDay.push(generalData.leadsDay[i]);
                    newGeneralData.spendDay.push(generalData.spendDay[i]);
                    newGeneralData.conversionDay.push(generalData.conversionDay[i]);
                    newGeneralData.maxCPL.push(generalData.maxCPL[i]);
                    newGeneralData.cplCumulative.push(generalData.cplCumulative[i]);
                    newGeneralData.cplCumulativeColors.push(
                        generalData.cplCumulativeColors[i]
                    );
                    newGeneralData.cplCumulativeArrows.push(
                        generalData.cplCumulativeArrows[i]
                    );
                    newGeneralData.groups.push(generalData.groups[i]);
                    newGeneralData.buyers.push(generalData.buyers[i]);
                    newGeneralData.accounts.push(generalData.accounts[i]);
                    newGeneralData.freq.push(generalData.freq[i]);
                    newGeneralData.ctr.push(generalData.ctr[i]);
                    newGeneralData.cpm.push(generalData.cpm[i]);
                    newGeneralData.linkClicks.push(generalData.linkClicks[i]);
                    newGeneralData.cpc.push(generalData.cpc[i]);
                    newGeneralData.avgWatchTime.push(generalData.avgWatchTime[i]);
                    newGeneralData.videoName.push(generalData.videoName[i]);
                    newGeneralData.siteUrl.push(generalData.siteUrl[i]);
                    newGeneralData.budget.push(generalData.budget[i]);
                }
            }
        });

        Object.assign(generalData, newGeneralData);

        // НОВАЯ СТРУКТУРА: Креатив → Кампания → Группа → Объявление
        console.log("🌲 Processing multi-level hierarchy data by creatives...");
        const buyerGroupsData = {};

        // Собираем все кампании для каждого креатива
        const creativeCampaignsMap = {}; // { creative: Set(campaigns) }
        const campaignGroupsMap = {}; // { "creative:::campaign": Set(groups) }
        const groupAdsMap = {}; // { "creative:::group": Set(ads) }

        // Создаем новые структуры данных для креативов
        const resultMapByCreative = {};
        const fbDataMapByCreative = {};
        const resultMapByCreativeCampaign = {};
        const fbDataMapByCreativeCampaign = {};
        const resultMapByCreativeCampaignGroup = {};
        const fbDataMapByCreativeCampaignGroup = {};
        const resultMapByCreativeGroupAd = {};
        const fbDataMapByCreativeGroupAd = {};

        allRows.forEach((row) => {
            const trackerName = String(row.campaign_name_tracker || "").trim();
            const campaignName = String(row.campaign_name || "").trim();
            const groupName = String(row.adv_group_name || "").trim();
            const advName = String(row.adv_name || "").trim();
            const videoName = String(row.video_name || "").trim();
            const groupId = String(row.adv_group_id || "").trim();
            const dateObj = new Date(row.adv_date);

            if (isNaN(dateObj.getTime()) || !trackerName.includes(article)) return;
            if (!videoName || videoName.trim() === "") return;

            const dateStr = Utilities.formatDate(dateObj, "Europe/Kiev", "yyyy-MM-dd");

            let buyerInfo = null;
            if (trackerName && trackerName.includes(article)) {
                buyerInfo = parseCampaignName(trackerName);
            } else if (campaignName && campaignToBuyerMap[campaignName]) {
                buyerInfo = campaignToBuyerMap[campaignName];
            } else if (groupId && adGroupToBuyerMap[groupId]) {
                buyerInfo = adGroupToBuyerMap[groupId];
            }

            if (!buyerInfo || buyerInfo.article !== article) return;

            const leads = Number(row.valid) || 0;
            const spend = Number(row.cost) || 0;
            const siteClicks = Number(row.clicks_on_link_tracker) || 0;
            const hasMetrics = campaignName || groupId;

            // Обработка данных для креатива - собираем связи
            if (videoName && campaignName) {
                if (!creativeCampaignsMap[videoName])
                    creativeCampaignsMap[videoName] = new Set();
                creativeCampaignsMap[videoName].add(campaignName);

                const campaignKey = `${videoName}:::${campaignName}`;
                if (groupName) {
                    if (!campaignGroupsMap[campaignKey])
                        campaignGroupsMap[campaignKey] = new Set();
                    campaignGroupsMap[campaignKey].add(groupName);

                    const groupKey = `${videoName}:::${groupName}`;
                    if (advName) {
                        if (!groupAdsMap[groupKey])
                            groupAdsMap[groupKey] = new Set();
                        groupAdsMap[groupKey].add(advName);
                    }
                }
            }

            // ДАННЫЕ ЛИДОВ И РАСХОДОВ - собираем для всех уровней
            if (leads > 0 || spend > 0) {
                console.log(`💰 Processing creative data for: ${videoName}, leads: ${leads}, spend: ${spend}, date: ${dateStr}`);

                // По креативам - ВСЕГДА добавляем данные если есть videoName
                if (!resultMapByCreative[videoName]) resultMapByCreative[videoName] = {};
                if (!resultMapByCreative[videoName][dateStr])
                    resultMapByCreative[videoName][dateStr] = { leads: 0, spend: 0 };
                resultMapByCreative[videoName][dateStr].leads += leads;
                resultMapByCreative[videoName][dateStr].spend += spend;

                console.log(`💰 Added to creative ${videoName}: total leads=${resultMapByCreative[videoName][dateStr].leads}, total spend=${resultMapByCreative[videoName][dateStr].spend}`);

                // Креатив → Кампания
                if (videoName && campaignName) {
                    const creativeCampaignKey = `${videoName}:::${campaignName}`;
                    if (!resultMapByCreativeCampaign[creativeCampaignKey])
                        resultMapByCreativeCampaign[creativeCampaignKey] = {};
                    if (!resultMapByCreativeCampaign[creativeCampaignKey][dateStr])
                        resultMapByCreativeCampaign[creativeCampaignKey][dateStr] = {
                            leads: 0,
                            spend: 0,
                        };
                    resultMapByCreativeCampaign[creativeCampaignKey][dateStr].leads += leads;
                    resultMapByCreativeCampaign[creativeCampaignKey][dateStr].spend += spend;
                }

                // Креатив → Кампания → Группа
                if (videoName && campaignName && groupName) {
                    const creativeCampaignGroupKey = `${videoName}:::${campaignName}:::${groupName}`;
                    if (!resultMapByCreativeCampaignGroup[creativeCampaignGroupKey])
                        resultMapByCreativeCampaignGroup[creativeCampaignGroupKey] = {};
                    if (!resultMapByCreativeCampaignGroup[creativeCampaignGroupKey][dateStr])
                        resultMapByCreativeCampaignGroup[creativeCampaignGroupKey][dateStr] = {
                            leads: 0,
                            spend: 0,
                        };
                    resultMapByCreativeCampaignGroup[creativeCampaignGroupKey][dateStr].leads += leads;
                    resultMapByCreativeCampaignGroup[creativeCampaignGroupKey][dateStr].spend += spend;
                }

                // Креатив → Группа → Объявление
                if (videoName && groupName && advName) {
                    const creativeGroupAdKey = `${videoName}:::${groupName}:::${advName}`;
                    if (!resultMapByCreativeGroupAd[creativeGroupAdKey])
                        resultMapByCreativeGroupAd[creativeGroupAdKey] = {};
                    if (!resultMapByCreativeGroupAd[creativeGroupAdKey][dateStr])
                        resultMapByCreativeGroupAd[creativeGroupAdKey][dateStr] = {
                            leads: 0,
                            spend: 0,
                        };
                    resultMapByCreativeGroupAd[creativeGroupAdKey][dateStr].leads += leads;
                    resultMapByCreativeGroupAd[creativeGroupAdKey][dateStr].spend += spend;
                }
            }

            // FACEBOOK МЕТРИКИ - добавляем для всех уровней если есть метрики
            if (hasMetrics && (campaignName || groupId)) {
                console.log(`📘 Processing creative Facebook metrics for: ${videoName}, date: ${dateStr}`);

                function addFacebookMetrics(targetObject, dateKey) {
                    if (!targetObject[dateKey]) {
                        targetObject[dateKey] = createFacebookMetricsObject();
                    }
                    targetObject[dateKey].adId.push(row.adv_id !== undefined && row.adv_id !== null ? String(row.adv_id) : "");
                    targetObject[dateKey].freq.push(row.frequency !== undefined && row.frequency !== null ? String(row.frequency) : "");
                    targetObject[dateKey].ctr.push(row.ctr !== undefined && row.ctr !== null ? String(row.ctr) : "");
                    targetObject[dateKey].cpm.push(row.cpm !== undefined && row.cpm !== null ? String(row.cpm) : "");
                    targetObject[dateKey].linkClicks.push(row.clicks_on_link_tracker !== undefined && row.clicks_on_link_tracker !== null ? String(row.clicks_on_link_tracker) : "");
                    targetObject[dateKey].cpc.push(row.cpc !== undefined && row.cpc !== null ? String(row.cpc) : "");
                    targetObject[dateKey].avgWatchTime.push(row.average_time_on_video !== undefined && row.average_time_on_video !== null ? String(row.average_time_on_video) : "");
                    targetObject[dateKey].videoName.push(videoName || "");
                    targetObject[dateKey].siteUrl.push(String(row.target_url || ""));
                    targetObject[dateKey].budget.push(row.adv_group_budjet !== undefined && row.adv_group_budjet !== null ? String(row.adv_group_budjet) : "");
                }

                // ПО КРЕАТИВАМ - ВСЕГДА добавляем если есть videoName
                if (!fbDataMapByCreative[videoName])
                    fbDataMapByCreative[videoName] = {};
                addFacebookMetrics(fbDataMapByCreative[videoName], dateStr);

                console.log(`📘 Added Facebook metrics to creative ${videoName} for date ${dateStr}`);

                // КРЕАТИВ → КАМПАНИЯ
                if (videoName && campaignName) {
                    const creativeCampaignKey = `${videoName}:::${campaignName}`;
                    if (!fbDataMapByCreativeCampaign[creativeCampaignKey])
                        fbDataMapByCreativeCampaign[creativeCampaignKey] = {};
                    addFacebookMetrics(fbDataMapByCreativeCampaign[creativeCampaignKey], dateStr);
                }

                // КРЕАТИВ → КАМПАНИЯ → ГРУППА
                if (videoName && campaignName && groupName) {
                    const creativeCampaignGroupKey = `${videoName}:::${campaignName}:::${groupName}`;
                    if (!fbDataMapByCreativeCampaignGroup[creativeCampaignGroupKey])
                        fbDataMapByCreativeCampaignGroup[creativeCampaignGroupKey] = {};
                    addFacebookMetrics(fbDataMapByCreativeCampaignGroup[creativeCampaignGroupKey], dateStr);
                }

                // КРЕАТИВ → ГРУППА → ОБЪЯВЛЕНИЕ
                if (videoName && groupName && advName) {
                    const creativeGroupAdKey = `${videoName}:::${groupName}:::${advName}`;
                    if (!fbDataMapByCreativeGroupAd[creativeGroupAdKey])
                        fbDataMapByCreativeGroupAd[creativeGroupAdKey] = {};
                    addFacebookMetrics(fbDataMapByCreativeGroupAd[creativeGroupAdKey], dateStr);
                }
            }
        });

        // Создаем иерархическую структуру по креативам
        Object.keys(creativeCampaignsMap).forEach((creativeName) => {
            console.log(`🎬 Processing creative: ${creativeName}`);

            buyerGroupsData[creativeName] = {
                buyerData: processSegment(
                    creativeName,
                    resultMapByCreative,
                    fbDataMapByCreative,
                    "creative"
                ),
                campaigns: {},
            };

            // Для каждого креатива находим его кампании
            if (creativeCampaignsMap[creativeName]) {
                Array.from(creativeCampaignsMap[creativeName]).forEach((campaignName) => {
                    console.log(`📺 Processing campaign: ${campaignName} for creative: ${creativeName}`);

                    const creativeCampaignKey = `${creativeName}:::${campaignName}`;
                    const campaignData = processSegment(
                        creativeCampaignKey,
                        resultMapByCreativeCampaign,
                        fbDataMapByCreativeCampaign,
                        "campaign"
                    );

                    buyerGroupsData[creativeName].campaigns[campaignName] = {
                        campaignData: campaignData,
                        groups: {}
                    };

                    // Для каждой кампании находим её группы
                    if (campaignGroupsMap[creativeCampaignKey]) {
                        Array.from(campaignGroupsMap[creativeCampaignKey]).forEach((groupName) => {
                            console.log(`📁 Processing group: ${groupName} for campaign: ${campaignName}`);

                            const creativeCampaignGroupKey = `${creativeName}:::${campaignName}:::${groupName}`;
                            const groupData = processSegment(
                                creativeCampaignGroupKey,
                                resultMapByCreativeCampaignGroup,
                                fbDataMapByCreativeCampaignGroup,
                                "group"
                            );

                            buyerGroupsData[creativeName].campaigns[campaignName].groups[groupName] = {
                                groupData: groupData,
                                ads: {}
                            };

                            // Для каждой группы находим её объявления
                            const groupKey = `${creativeName}:::${groupName}`;
                            if (groupAdsMap[groupKey]) {
                                Array.from(groupAdsMap[groupKey]).forEach((advName) => {
                                    console.log(`📄 Processing ad: ${advName} for group: ${groupName}`);

                                    const creativeGroupAdKey = `${creativeName}:::${groupName}:::${advName}`;
                                    const adData = processSegment(
                                        creativeGroupAdKey,
                                        resultMapByCreativeGroupAd,
                                        fbDataMapByCreativeGroupAd,
                                        "ad"
                                    );

                                    buyerGroupsData[creativeName].campaigns[campaignName].groups[groupName].ads[advName] = adData;
                                });
                            }
                        });
                    }
                });
            }
        });

        console.log(
            "🌲 Buyer-group hierarchy created:",
            Object.keys(buyerGroupsData).length,
            "buyers"
        );
        console.log("🎯 Final structure overview:");
        Object.keys(buyerGroupsData).forEach((buyer) => {
            console.log(
                `  👤 ${buyer}: ${Object.keys(buyerGroupsData[buyer].campaigns || {}).length
                } campaigns`
            );
        });

        console.log("🔍 DEBUG: Checking creative data structure:");
        Object.keys(buyerGroupsData).forEach(creativeName => {
            console.log(`🔍 Creative: ${creativeName}`);
            console.log(`🔍 Has buyerData:`, !!buyerGroupsData[creativeName].buyerData);
            if (buyerGroupsData[creativeName].buyerData) {
                console.log(`🔍 Data dates:`, buyerGroupsData[creativeName].buyerData.data.dates.length);
                console.log(`🔍 Leads data:`, buyerGroupsData[creativeName].buyerData.data.leadsDay.slice(0, 5));
                console.log(`🔍 Spend data:`, buyerGroupsData[creativeName].buyerData.data.spendDay.slice(0, 5));
            }
        });

        // Общие метрики
        const crValue =
            totalClicksAll > 0 ? (totalLeadsAll / totalClicksAll) * 100 : 0;
        const crStr = crValue.toFixed(2).replace(".", ",") + "%";

        console.log("🎉 Analysis completed successfully!");
        console.log("📊 Total unique videos found:", globalVideos.size);
        console.log("👥 Buyers with groups:", Object.keys(buyerGroupsData));

        // Создаем структуру календаря метрик
        console.log("📅 Создаем структуру календаря метрик...");
        const calendarData = {};

        allRows.forEach((row) => {
            const trackerName = String(row.campaign_name_tracker || "").trim();
            const campaignName = String(row.campaign_name || "").trim();
            const groupName = String(row.adv_group_name || "").trim();
            const adName = String(row.adv_name || "").trim();
            const videoName = String(row.video_name || "").trim();
            const dateObj = new Date(row.adv_date);

            if (isNaN(dateObj.getTime()) || !trackerName.includes(article)) return;

            const dateStr = Utilities.formatDate(dateObj, "Europe/Kiev", "dd.MM.yyyy");
            const leads = Number(row.valid) || 0;
            const spend = Number(row.cost) || 0;
            const cpl = leads > 0 ? spend / leads : 0;

            // Пропускаем записи без названия видео
            if (!videoName || videoName.trim() === "") return;

            // Используем только название видео как ключ верхнего уровня
            const topLevelKey = videoName.trim();

            // Создаем структуру видео
            if (!calendarData[topLevelKey]) {
                calendarData[topLevelKey] = {
                    dates: [],
                    campaigns: {}
                };
            }

            // Добавляем дату если её нет
            if (!calendarData[topLevelKey].dates.includes(dateStr)) {
                calendarData[topLevelKey].dates.push(dateStr);
            }

            // Создаем структуру кампании
            if (campaignName && !calendarData[topLevelKey].campaigns[campaignName]) {
                calendarData[topLevelKey].campaigns[campaignName] = {
                    dates: [],
                    cpl: [],
                    leads: [],
                    spend: [],
                    ctr: [],
                    cpm: [],
                    clicks: [],
                    impressions: [],
                    groups: {}
                };
            }

            // Создаем структуру группы
            if (campaignName && groupName && !calendarData[topLevelKey].campaigns[campaignName].groups[groupName]) {
                calendarData[topLevelKey].campaigns[campaignName].groups[groupName] = {
                    dates: [],
                    cpl: [],
                    leads: [],
                    spend: [],
                    ctr: [],
                    cpm: [],
                    clicks: [],
                    impressions: [],
                    ads: {}
                };
            }

            // Создаем структуру объявления
            if (campaignName && groupName && adName && !calendarData[topLevelKey].campaigns[campaignName].groups[groupName].ads[adName]) {
                calendarData[topLevelKey].campaigns[campaignName].groups[groupName].ads[adName] = {
                    dates: [],
                    cpl: [],
                    leads: [],
                    spend: [],
                    ctr: [],
                    cpm: [],
                    clicks: [],
                    impressions: []
                };
            }
        });

        // Заполняем данные по датам
        Object.keys(calendarData).forEach(videoKey => {
            const videoData = calendarData[videoKey];
            videoData.dates.sort((a, b) => {
            const [dayA, monthA, yearA] = a.split('.').map(Number);
            const [dayB, monthB, yearB] = b.split('.').map(Number);
            return yearA - yearB || monthA - monthB || dayA - dayB;
        });

            Object.keys(videoData.campaigns).forEach(campaignName => {
                const campaignData = videoData.campaigns[campaignName];

                // Инициализируем массивы для всех дат
                videoData.dates.forEach(date => {
                    campaignData.dates.push(date);
                    campaignData.cpl.push(0);
                    campaignData.leads.push(0);
                    campaignData.spend.push(0);
                    campaignData.ctr.push(0);
                    campaignData.cpm.push(0);
                    campaignData.clicks.push(0);
                    campaignData.impressions.push(0);
                });

                Object.keys(campaignData.groups).forEach(groupName => {
                    const groupData = campaignData.groups[groupName];

                    videoData.dates.forEach(date => {
                        groupData.dates.push(date);
                        groupData.cpl.push(0);
                        groupData.leads.push(0);
                        groupData.spend.push(0);
                        groupData.ctr.push(0);
                        groupData.cpm.push(0);
                        groupData.clicks.push(0);
                        groupData.impressions.push(0);
                    });

                    Object.keys(groupData.ads).forEach(adName => {
                        const adData = groupData.ads[adName];

                        videoData.dates.forEach(date => {
                            adData.dates.push(date);
                            adData.cpl.push(0);
                            adData.leads.push(0);
                            adData.spend.push(0);
                            adData.ctr.push(0);
                            adData.cpm.push(0);
                            adData.clicks.push(0);
                            adData.impressions.push(0);
                        });
                    });
                });
            });
        });

        // Заполняем фактические данные
        allRows.forEach((row) => {
            const trackerName = String(row.campaign_name_tracker || "").trim();
            const campaignName = String(row.campaign_name || "").trim();
            const groupName = String(row.adv_group_name || "").trim();
            const adName = String(row.adv_name || "").trim();
            const videoName = String(row.video_name || "").trim();
            const dateObj = new Date(row.adv_date);

            if (isNaN(dateObj.getTime()) || !trackerName.includes(article)) return;

            const dateStr = Utilities.formatDate(dateObj, "Europe/Kiev", "dd.MM.yyyy");
            const leads = Number(row.valid) || 0;
            const spend = Number(row.cost) || 0;
            const cpl = leads > 0 ? spend / leads : 0;

            // Facebook метрики
            const ctr = Number(row.ctr) || 0;
            const cpm = Number(row.cpm) || 0;
            const clicks = Number(row.clicks_on_link_tracker) || 0;
            const impressions = cpm > 0 && spend > 0 ? Math.round((spend / cpm) * 1000) : 0;

            // Пропускаем записи без названия видео
            if (!videoName || videoName.trim() === "") return;

            // Используем только название видео как ключ верхнего уровня
            const topLevelKey = videoName.trim();

            if (calendarData[topLevelKey]) {
                const dateIndex = calendarData[topLevelKey].dates.indexOf(dateStr);

                if (dateIndex >= 0 && campaignName && calendarData[topLevelKey].campaigns[campaignName]) {
                    const campaignData = calendarData[topLevelKey].campaigns[campaignName];
                    campaignData.cpl[dateIndex] += cpl;
                    campaignData.leads[dateIndex] += leads;
                    campaignData.spend[dateIndex] += spend;
                    campaignData.ctr[dateIndex] += ctr;
                    campaignData.cpm[dateIndex] += cpm;
                    campaignData.clicks[dateIndex] += clicks;
                    campaignData.impressions[dateIndex] += impressions;

                    if (groupName && campaignData.groups[groupName]) {
                        const groupData = campaignData.groups[groupName];
                        groupData.cpl[dateIndex] += cpl;
                        groupData.leads[dateIndex] += leads;
                        groupData.spend[dateIndex] += spend;
                        groupData.ctr[dateIndex] += ctr;
                        groupData.cpm[dateIndex] += cpm;
                        groupData.clicks[dateIndex] += clicks;
                        groupData.impressions[dateIndex] += impressions;

                        if (adName && groupData.ads[adName]) {
                            const adData = groupData.ads[adName];
                            adData.cpl[dateIndex] += cpl;
                            adData.leads[dateIndex] += leads;
                            adData.spend[dateIndex] += spend;
                            adData.ctr[dateIndex] += ctr;
                            adData.cpm[dateIndex] += cpm;
                            adData.clicks[dateIndex] += clicks;
                            adData.impressions[dateIndex] += impressions;
                        }
                    }
                }
            }
        });

        const finalResult = {
            article: article,
            generalData: generalData,
            buyerGroupsData: buyerGroupsData,
            calendarData: calendarData,
            generalMetrics: {
                activeDays: activeDays,
                daysInNorm: daysInNorm,
                daysBelowAllowed: daysBelowAllowed,
                totalGroups: globalGroups.size,
                totalBuyers: globalBuyers.size,
                totalAccounts: globalAccounts.size,
                cr: crStr,
                videos: globalVideos.size,
                sites: globalSites.size,
                displayMaxCPL: displayMaxCPL.toFixed(2),
                displayCPL_ROI_minus5: displayCPL_ROI_minus5.toFixed(2),
                groupNames: sortMetricsByBuyers(
                    Array.from(globalGroups),
                    Array.from(globalBuyers),
                    buyerGroupsMap
                ),
                buyerNames: Array.from(globalBuyers).join("\n"),
                accountNames: Array.from(globalAccounts).join("\n"),
                videoNames: sortVideosByBuyers(
                    Array.from(globalBuyers),
                    buyerVideosMap,
                    globalVideos
                ),
                siteUrls: sortSitesByBuyers(
                    Array.from(globalBuyers),
                    buyerSitesMap,
                    globalSites
                ),
                status: status,
                season: season,
                category: category,
                stock: stock,
                stockDays: stockDays,
                efficiencyZone: efficiencyZoneFormatted,
                zoneAB: zoneABFormatted,
                zoneAC: zoneACFormatted,
                zoneAD: zoneADFormatted,
                zoneAE: zoneAEFormatted,
            },
        };

        console.log("🔥 =================================");
        console.log("🔥 ВОЗВРАЩАЕМ РЕЗУЛЬТАТ");
        console.log("🔥 Артикул:", finalResult.article);
        console.log("🔥 Количество дат:", finalResult.generalData.dates.length);
        console.log(
            "🔥 Количество байеров:",
            Object.keys(finalResult.buyerGroupsData).length
        );
        console.log("🔥 =================================");

        return finalResult;
    } catch (error) {
        console.log("🔥 =================================");
        console.log("🔥 ОШИБКА В buildChartForArticle");
        console.log("🔥 Тип ошибки:", typeof error);
        console.log("🔥 Сообщение ошибки:", error.message);
        console.log("🔥 Полная ошибка:", error);
        console.log("🔥 Stack trace:", error.stack);
        console.log("🔥 =================================");

        // Если это уже наша пользовательская ошибка, передаем как есть
        if (
            error.message &&
            (error.message.includes("📊") ||
                error.message.includes("🔌") ||
                error.message.includes("🚨") ||
                error.message.includes("🔧") ||
                error.message.includes("📝") ||
                error.message.includes("📋") ||
                error.message.includes("🔒"))
        ) {
            console.log("🔥 Перебрасываем пользовательскую ошибку");
            throw error;
        }

        // Для всех остальных ошибок
        console.log("🔥 Создаем общую ошибку");
        throw new Error(
            `⚠️ Произошла неожиданная ошибка!\n\nАртикул: ${article}\n\nТехническая информация:\n${error.message}\n\nРекомендации:\n• Обновите страницу и попробуйте снова\n• Проверьте правильность введенных данных\n• Обратитесь к администратору`
        );
    }
}
