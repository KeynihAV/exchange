package delivery

import (
	"fmt"
	"log"
	"net/http"
	"strconv"

	clientPkg "github.com/KeynihAV/exchange/pkg/broker/client"
	clientsUsecasePkg "github.com/KeynihAV/exchange/pkg/broker/client/usecase"
	configPkg "github.com/KeynihAV/exchange/pkg/broker/config"
	dealUsecasePkg "github.com/KeynihAV/exchange/pkg/broker/deal/usecase"
	statsRepoPkg "github.com/KeynihAV/exchange/pkg/broker/stats/repo"
	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api"
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
	dealsManager *dealUsecasePkg.DealsManager) error {

	go listenWebhook(config.ListenAddr)

	bot, err := tgbotapi.NewBotAPI(config.BotToken)
	if err != nil {
		return fmt.Errorf("not create bot api: %v", err)
	}

	resp, err := bot.SetWebhook(tgbotapi.NewWebhook(config.WebhookURL))
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
			}
			if err != nil {
				fmt.Printf("error processing message: %v\n", err)
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
				fmt.Printf("error creating client: %v\n", err)
				continue
			}
			switch cmdTxt := update.Message.Command(); {
			case cmdTxt == "stats" || cmdTxt == "buy" || cmdTxt == "sell":
				msg := tgbotapi.NewMessage(chatID, "Выберите инструмент")
				msg.ReplyMarkup = tickersKeyboard(config)
				if _, err = bot.Send(msg); err != nil {
					fmt.Printf("error send msg: %v", err)
				}

				if cmdTxt == "buy" || cmdTxt == "sell" {
					dialog.CurrentOrder, err = tgBot.dealsManager.NewOrder(cmdTxt, config.BrokerID, int32(client.ID))
					if err != nil {
						fmt.Printf("not create new order")
						continue
					}
				}
			}

			dialog.CurrentCommand = update.Message.Command()
			tgBot.clientsManager.ActiveDialogs[chatID] = dialog
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
				orderID, err := tgBot.dealsManager.CreateOrder(dialog.CurrentOrder)
				if err != nil {
					fmt.Printf("order creation error: %v", err)
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

func listenWebhook(addr string) {
	err := http.ListenAndServe(addr, nil)
	if err != nil {
		log.Fatalf("http server not started: %v", err)
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

func tickersKeyboard(config *configPkg.Config) tgbotapi.InlineKeyboardMarkup {
	row := tgbotapi.NewInlineKeyboardRow()
	for _, ticker := range config.Tickers {
		row = append(row, tgbotapi.NewInlineKeyboardButtonData(ticker, ticker))
	}
	return tgbotapi.NewInlineKeyboardMarkup(row)
}
