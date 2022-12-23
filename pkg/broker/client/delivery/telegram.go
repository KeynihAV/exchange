package delivery

import (
	"fmt"
	"log"
	"net/http"

	clientsUsecasePkg "github.com/KeynihAV/exchange/pkg/broker/client/usecase"
	configPkg "github.com/KeynihAV/exchange/pkg/broker/config"
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

func StartTgBot(config *configPkg.Config, clientsManager *clientsUsecasePkg.ClientsManager) error {
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

		switch update.Message.Command() {
		case "help":
			msg.Text = "помощь"
		}

		msg.ReplyMarkup = keyboard
		msg.DisableWebPagePreview = true

		if _, err := bot.Send(msg); err != nil {
			fmt.Printf("error send msg: %v\n", err)
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
