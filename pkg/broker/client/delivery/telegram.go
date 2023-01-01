package delivery

import (
	"fmt"
	"net/http"
	"strconv"
	"time"

	clientPkg "github.com/KeynihAV/exchange/pkg/broker/client"
	clientsUsecasePkg "github.com/KeynihAV/exchange/pkg/broker/client/usecase"
	dealUsecasePkg "github.com/KeynihAV/exchange/pkg/broker/deal/usecase"
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
}

func StartTgBot(
	config *configPkg.Config,
	clientsManager *clientsUsecasePkg.ClientsManager,
	statsRepo *statsRepoPkg.StatsRepo,
	dealsManager *dealUsecasePkg.DealsManager,
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

	tgBot := &brokerTgBot{clientsManager: clientsManager, statsRepo: statsRepo, dealsManager: dealsManager}

	chUpdates := bot.ListenForWebhook("/")
	for update := range chUpdates {
		var messages []string

		if update.CallbackQuery != nil {
			chatID := update.CallbackQuery.Message.Chat.ID
			dialog := tgBot.clientsManager.ActiveDialogs[chatID]
			switch cmdTxt := dialog.CurrentCommand; {
			case cmdTxt == "stats":
				messages, err = tgBot.getStats(update.CallbackQuery.Data)
				dialog.CurrentCommand = ""
			case cmdTxt == "buy" || cmdTxt == "sell":
				dialog.CurrentOrder.Ticker = update.CallbackQuery.Data
				dialog.LastMsg = "Укажите цену"
				messages = append(messages, dialog.LastMsg)
			case cmdTxt == "orders":
				messages, err = tgBot.cancelOrder(update.CallbackQuery.Data, config)
			}
			if err != nil {
				logger.Zap.Error("processing callback",
					zap.String("logger", "tgbot"),
					zap.String("msg", update.CallbackQuery.Data),
					zap.String("err", err.Error()),
				)
				bot.Send(tgbotapi.NewMessage(chatID, err.Error()))
				continue
			}
			for _, msg := range messages {
				bot.Send(tgbotapi.NewMessage(chatID, msg))
			}
		} else if update.Message.IsCommand() {
			chatID := update.Message.Chat.ID
			dialog := &clientPkg.Dialog{}
			client, err := clientsManager.CheckAndCreateClient(update.Message.From.UserName, int(chatID))
			if err != nil {
				logger.Zap.Error("creating client",
					zap.String("logger", "tgbot"),
					zap.String("msg", update.Message.Command()),
					zap.String("err", err.Error()),
				)
				continue
			}
			switch cmdTxt := update.Message.Command(); {
			case cmdTxt == "stats" || cmdTxt == "buy" || cmdTxt == "sell":
				msg := tgbotapi.NewMessage(chatID, "Выберите инструмент")
				msg.ReplyMarkup = tickersKeyboard(config)
				if _, err = bot.Send(msg); err != nil {
					logger.Zap.Error("send msg",
						zap.String("logger", "tgbot"),
						zap.String("msg", update.Message.Command()),
						zap.String("err", err.Error()),
					)
					continue
				}

				if cmdTxt == "buy" || cmdTxt == "sell" {
					dialog.CurrentOrder, err = tgBot.dealsManager.NewOrder(cmdTxt, config.Broker.ID, int32(client.ID))
					if err != nil {
						logger.Zap.Error("create order",
							zap.String("logger", "tgbot"),
							zap.String("msg", cmdTxt),
							zap.String("err", err.Error()),
						)
						continue
					}
				}
			case cmdTxt == "orders":
				replyMarkup, err := tgBot.ordersKeyboard(client.ID)
				if err != nil {
					logger.Zap.Error("cancel order",
						zap.String("logger", "tgbot"),
						zap.String("msg", cmdTxt),
						zap.String("err", err.Error()),
					)
					bot.Send(tgbotapi.NewMessage(chatID, "Не удалось отменить заявку"))
					continue
				}
				msg := tgbotapi.NewMessage(chatID, "Ваши заявки: (нажмите на заявку для отмены)")
				msg.ReplyMarkup = replyMarkup
				bot.Send(msg)
			case cmdTxt == "balance":
				messages, err = tgBot.getBalance(client)
			}

			dialog.CurrentCommand = update.Message.Command()
			tgBot.clientsManager.ActiveDialogs[chatID] = dialog

			if err != nil {
				logger.Zap.Error("processing message",
					zap.String("logger", "tgbot"),
					zap.String("msg", update.Message.Command()),
					zap.String("err", err.Error()),
				)
				bot.Send(tgbotapi.NewMessage(chatID, err.Error()))
				continue
			}
			for _, msg := range messages {
				bot.Send(tgbotapi.NewMessage(chatID, msg))
			}
		} else {
			chatID := update.Message.Chat.ID
			dialog := tgBot.clientsManager.ActiveDialogs[chatID]
			switch dialog.LastMsg {
			case "Укажите цену":
				price, err := strconv.ParseFloat(update.Message.Text, 32)
				if err != nil {
					bot.Send(tgbotapi.NewMessage(chatID, fmt.Sprintf("Не правильно введена цена: %v\n Попробуйте еще", err.Error())))
					continue
				}

				dialog.CurrentOrder.Price = float32(price)
				dialog.LastMsg = "Укажите объем"
				messages = append(messages, dialog.LastMsg)
			case "Укажите объем":
				volume, err := strconv.ParseInt(update.Message.Text, 10, 32)
				if err != nil {
					bot.Send(tgbotapi.NewMessage(chatID, fmt.Sprintf("Не правильно введен объем: %v\n Попробуйте еще", err.Error())))
					continue
				}
				dialog.CurrentOrder.Volume = int32(volume)
				orderID, err := tgBot.dealsManager.CreateOrder(dialog.CurrentOrder, config)
				if err != nil {
					logger.Zap.Error("creating order",
						zap.String("logger", "tgbot"),
						zap.Int32("clientID", dialog.CurrentOrder.ClientID),
						zap.String("err", err.Error()),
					)
					bot.Send(tgbotapi.NewMessage(chatID, fmt.Sprintf("Не удалось создать заявку: %v", err)))
					continue
				}
				messages = append(messages, fmt.Sprintf("Создана заявка с номером %v", orderID))
			}
			for _, msg := range messages {
				bot.Send(tgbotapi.NewMessage(chatID, msg))
			}
		}
	}

	return nil
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
