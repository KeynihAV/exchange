package delivery

import (
	"fmt"
	"net/http"
	"strconv"
	"time"

	clientPkg "github.com/KeynihAV/exchange/pkg/broker/client"
	clientsUsecasePkg "github.com/KeynihAV/exchange/pkg/broker/client/usecase"
	dealUsecasePkg "github.com/KeynihAV/exchange/pkg/broker/deal/usecase"
	sessUsecasePkg "github.com/KeynihAV/exchange/pkg/broker/session/usecase"
	statsRepoPkg "github.com/KeynihAV/exchange/pkg/broker/stats/repo"
	configPkg "github.com/KeynihAV/exchange/pkg/config"
	"github.com/KeynihAV/exchange/pkg/logging"
	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api"
	"go.uber.org/zap"
)

type brokerTgBot struct {
	clientsManager *clientsUsecasePkg.ClientsManager
	statsRepo      *statsRepoPkg.StatsRepo
	dealsManager   *dealUsecasePkg.DealsManager
	sessManager    *sessUsecasePkg.SessionsManager
}

func StartTgBot(
	config *configPkg.Config,
	clientsManager *clientsUsecasePkg.ClientsManager,
	statsRepo *statsRepoPkg.StatsRepo,
	dealsManager *dealUsecasePkg.DealsManager,
	sessManager *sessUsecasePkg.SessionsManager,
	logger *logging.Logger) error {

	go listenWebhook(":"+strconv.Itoa(config.HTTP.Port), logger)

	bot, err := tgbotapi.NewBotAPI(config.Bot.Token)
	if err != nil {
		return fmt.Errorf("not create bot api: %v", err)
	}

	resp, err := bot.SetWebhook(tgbotapi.NewWebhook(config.Bot.WebhookURL))
	if err != nil {
		return fmt.Errorf("not set webhook: %v", err)
	}
	if !resp.Ok {
		return fmt.Errorf("error creating webhook. code: %v, description: %v", resp.ErrorCode, resp.Description)
	}
	u := tgbotapi.NewUpdate(0)
	u.Timeout = 20

	tgBot := &brokerTgBot{clientsManager: clientsManager, statsRepo: statsRepo, dealsManager: dealsManager, sessManager: sessManager}

	chUpdates := bot.ListenForWebhook("/")
	for update := range chUpdates {
		var messages []tgbotapi.MessageConfig
		var chatID int64
		var inputMsg, processing string

		if update.UpdateID == 0 {
			continue
		}

		if update.CallbackQuery != nil {
			chatID = update.CallbackQuery.Message.Chat.ID
			inputMsg = update.CallbackQuery.Data
			processing = "callback"

			messages, err = tgBot.processingCallback(chatID, inputMsg, config)

		} else if update.Message.IsCommand() {
			chatID = update.Message.Chat.ID
			inputMsg = update.Message.Command()
			processing = "command"
			messages, err = tgBot.processingCommand(chatID, update.Message.From.UserName, inputMsg, config)

		} else {
			chatID = update.Message.Chat.ID
			inputMsg = update.Message.Text
			processing = "message"

			messages, err = tgBot.processingMessages(chatID, inputMsg, config)
		}

		if err != nil {
			logger.Zap.Error("processing "+processing,
				zap.String("logger", "tgbot"),
				zap.String("msg", inputMsg),
				zap.String("err", err.Error()),
			)
			bot.Send(tgbotapi.NewMessage(chatID, err.Error()))
		}
		for _, msg := range messages {
			bot.Send(msg)
		}
	}

	return nil
}

func (tgBot *brokerTgBot) processingCommand(chatID int64, userName string, inputMsg string, config *configPkg.Config) ([]tgbotapi.MessageConfig, error) {
	dialog := &clientPkg.Dialog{}
	var messages []tgbotapi.MessageConfig

	_, err := tgBot.sessManager.GetSession(chatID)
	if err != nil {
		registerURL := fmt.Sprintf("%v?client_id=%v&redirect_uri=%v&response_type=code&state=%v",
			config.Bot.Auth.Url, config.Bot.Auth.App_id, config.Bot.Auth.Redirect_uri, chatID)
		messages = append(messages, tgbotapi.NewMessage(chatID, "Авторизуйтесь для продолжения: "+registerURL))
		return messages, nil
	}
	client, err := tgBot.clientsManager.CheckAndCreateClient(userName, chatID)
	if err != nil {
		return messages, fmt.Errorf("creating client %v", err.Error())
	}

	switch cmdTxt := inputMsg; {
	case cmdTxt == "stats" || cmdTxt == "buy" || cmdTxt == "sell":
		msg := tgbotapi.NewMessage(chatID, "Выберите инструмент")
		msg.ReplyMarkup = tickersKeyboard(config)
		messages = append(messages, msg)
		if cmdTxt == "buy" || cmdTxt == "sell" {
			dialog.CurrentOrder, err = tgBot.dealsManager.NewOrder(cmdTxt, config.Broker.ID, int32(client.ID))
			if err != nil {
				return messages, fmt.Errorf("create order %v", err.Error())
			}
		}
	case cmdTxt == "orders":
		replyMarkup, err := tgBot.ordersKeyboard(client.ID)
		if err != nil {
			messages = append(messages, tgbotapi.NewMessage(chatID, "Не удалось отменить заявку"))
			return messages, fmt.Errorf("cancel order %v", err.Error())
		}

		msg := tgbotapi.NewMessage(chatID, "Ваши заявки: (нажмите на заявку для отмены)")
		msg.ReplyMarkup = replyMarkup
		messages = append(messages, msg)
	case cmdTxt == "balance":
		msgs, err := tgBot.getBalance(client)
		if err != nil {
			return messages, err
		}
		for _, msg := range msgs {
			messages = append(messages, tgbotapi.NewMessage(chatID, msg))
		}
	}

	dialog.CurrentCommand = inputMsg
	tgBot.clientsManager.ActiveDialogs[chatID] = dialog

	return messages, nil
}

func (tgBot *brokerTgBot) processingMessages(chatID int64, inputMsg string, config *configPkg.Config) ([]tgbotapi.MessageConfig, error) {
	dialog := tgBot.clientsManager.ActiveDialogs[chatID]
	var messages []tgbotapi.MessageConfig
	switch dialog.LastMsg {
	case "Укажите цену":
		price, err := strconv.ParseFloat(inputMsg, 32)
		if err != nil {
			return messages, fmt.Errorf("не правильно введена цена: %v\n Попробуйте еще", err.Error())
		}
		dialog.CurrentOrder.Price = float32(price)
		dialog.LastMsg = "Укажите объем"
		messages = append(messages, tgbotapi.NewMessage(chatID, dialog.LastMsg))
	case "Укажите объем":
		volume, err := strconv.ParseInt(inputMsg, 10, 32)
		if err != nil {
			return messages, fmt.Errorf("не правильно введен объем: %v\n Попробуйте еще", err.Error())
		}
		dialog.CurrentOrder.Volume = int32(volume)
		orderID, err := tgBot.dealsManager.CreateOrder(dialog.CurrentOrder, config)
		if err != nil {
			return messages, fmt.Errorf("не удалось создать заявку: %vе", err.Error())
		}
		messages = append(messages, tgbotapi.NewMessage(chatID, fmt.Sprintf("Создана заявка с номером %v", orderID)))
	}
	return messages, nil
}

func (tgBot *brokerTgBot) processingCallback(chatID int64, inputMsg string, config *configPkg.Config) ([]tgbotapi.MessageConfig, error) {
	var messages []tgbotapi.MessageConfig
	var err error
	dialog := tgBot.clientsManager.ActiveDialogs[chatID]
	switch cmdTxt := dialog.CurrentCommand; {
	case cmdTxt == "stats":
		msgs, err := tgBot.getStats(inputMsg)
		if err != nil {
			return messages, err
		}
		for _, msg := range msgs {
			messages = append(messages, tgbotapi.NewMessage(chatID, msg))
		}
		dialog.CurrentCommand = ""
	case cmdTxt == "buy" || cmdTxt == "sell":
		dialog.CurrentOrder.Ticker = inputMsg
		dialog.LastMsg = "Укажите цену"
		messages = append(messages, tgbotapi.NewMessage(chatID, dialog.LastMsg))
	case cmdTxt == "orders":
		msgs, err := tgBot.cancelOrder(inputMsg, config)
		if err != nil {
			return messages, err
		}
		for _, msg := range msgs {
			messages = append(messages, tgbotapi.NewMessage(chatID, msg))
		}
	}
	return messages, err
}

func listenWebhook(addr string, logger *logging.Logger) {
	err := http.ListenAndServe(addr, nil)
	if err != nil {
		logger.Zap.Fatal("error starting http server",
			zap.String("logger", "tgbot"),
			zap.String("err: ", err.Error()))
	}
}

func (tgBot *brokerTgBot) getStats(ticker string) ([]string, error) {
	stats, err := tgBot.statsRepo.GeStatsByTicker(ticker)
	if err != nil {
		return nil, fmt.Errorf("get stats %v", err)
	}

	messages := make([]string, 0)
	for _, stat := range stats {
		msgTxt := fmt.Sprintf("time: %v, open: %v, high: %v, low: %v, close: %v, volume:%v",
			stat.Time, stat.Open, stat.High, stat.Low, stat.Close, stat.Volume)
		messages = append(messages, msgTxt)
	}

	return messages, nil
}

func (tgBot *brokerTgBot) cancelOrder(callbackData string, config *configPkg.Config) ([]string, error) {
	orderID, err := strconv.ParseInt(callbackData, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("parseInt in cancel order %v", err)
	}

	err = tgBot.dealsManager.CancelOrder(orderID, config)
	if err != nil {
		return nil, fmt.Errorf("cancel order %v", err)
	}

	return []string{fmt.Sprintf("Заявка %v удалена", orderID)}, nil
}

func tickersKeyboard(config *configPkg.Config) tgbotapi.InlineKeyboardMarkup {
	row := tgbotapi.NewInlineKeyboardRow()
	for _, ticker := range config.Broker.Tickers {
		row = append(row, tgbotapi.NewInlineKeyboardButtonData(ticker, ticker))
	}
	return tgbotapi.NewInlineKeyboardMarkup(row)
}

func (tgBot *brokerTgBot) ordersKeyboard(clientID int) (tgbotapi.InlineKeyboardMarkup, error) {
	orders, err := tgBot.dealsManager.OrdersByClient(clientID)
	if err != nil {
		return tgbotapi.InlineKeyboardMarkup{}, err
	}

	markup := tgbotapi.NewInlineKeyboardMarkup()
	for _, order := range orders {
		status := "ожидает исполнения"
		if order.CompletedVolume > 0 {
			status = "частично исполнена"
		}
		orderType := "Покупка"
		if order.Type == "sell" {
			orderType = "Продажа"
		}
		orderDescr := fmt.Sprintf("%v №:%v от %v %v %vшт (%v)",
			orderType, order.ID, time.Unix(int64(order.Time), 0).Format("02 Jan 06 15:04"), order.Ticker, order.Volume, status)
		row := tgbotapi.NewInlineKeyboardRow(tgbotapi.NewInlineKeyboardButtonData(orderDescr, strconv.FormatInt(order.ID, 10)))
		markup.InlineKeyboard = append(markup.InlineKeyboard, row)
	}
	return markup, nil
}

func (tgBot *brokerTgBot) getBalance(client *clientPkg.Client) ([]string, error) {
	positions, err := tgBot.clientsManager.GetBalance(client)
	if err != nil {
		return nil, fmt.Errorf("error get balance: %v", err)
	}
	messages := make([]string, len(positions))

	for _, position := range positions {
		messages = append(messages, fmt.Sprintf("ticker: %v, volume: %v, total: %.2f, price(avg): %.2f",
			position.Ticker, position.Volume, position.Total, position.Price))
	}

	return messages, nil
}
