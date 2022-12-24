package delivery

import (
	"fmt"
	"log"
	"net/http"
	"strings"

	clientsUsecasePkg "github.com/KeynihAV/exchange/pkg/broker/client/usecase"
	configPkg "github.com/KeynihAV/exchange/pkg/broker/config"
	statsRepoPkg "github.com/KeynihAV/exchange/pkg/broker/stats/repo"
	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api"
)

var keyboard = tgbotapi.NewReplyKeyboard(
	tgbotapi.NewKeyboardButtonRow(
		tgbotapi.NewKeyboardButton("status"),
		tgbotapi.NewKeyboardButton("deal"),
		tgbotapi.NewKeyboardButton("cancel"),
		tgbotapi.NewKeyboardButton("history"),
	),
)

type brokerTgBot struct {
	clientsManager *clientsUsecasePkg.ClientsManager
	statsRepo      *statsRepoPkg.StatsRepo
}

func StartTgBot(config *configPkg.Config, clientsManager *clientsUsecasePkg.ClientsManager, statsRepo *statsRepoPkg.StatsRepo) error {
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

	tgBot := &brokerTgBot{clientsManager: clientsManager, statsRepo: statsRepo}

	chUpdates := bot.ListenForWebhook("/")
	for update := range chUpdates {
		if update.Message == nil || !update.Message.IsCommand() {
			continue
		}
		chatID := update.Message.Chat.ID
		msg := tgbotapi.NewMessage(chatID, update.Message.Text)
		_, err := clientsManager.CheckAndCreateClient(update.Message.From.UserName, update.Message.From.ID, chatID)
		if err != nil {
			fmt.Printf("error creating client: %v\n", err)
			continue
		}

		var messages []string
		switch update.Message.Command() {
		case "stats":
			messages, err = tgBot.getStats(update.Message.Text)
		}

		if err != nil {
			fmt.Printf("error processing message: %v\n", err)
			bot.Send(tgbotapi.NewMessage(chatID, err.Error()))
			continue
		}
		for _, msg := range messages {
			bot.Send(tgbotapi.NewMessage(chatID, msg))
		}
		msg.ReplyMarkup = keyboard
		msg.DisableWebPagePreview = true
	}

	return nil
}

func listenWebhook(addr string) {
	err := http.ListenAndServe(addr, nil)
	if err != nil {
		log.Fatalf("http server not started: %v", err)
	}
}

func (tgBot *brokerTgBot) getStats(msgTxt string) ([]string, error) {
	stats, err := tgBot.statsRepo.GeStatsByTicker(strings.TrimPrefix(msgTxt, "/stats "))
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
