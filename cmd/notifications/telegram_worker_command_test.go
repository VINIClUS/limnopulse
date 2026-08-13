package main

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
)

type fakeTelegramSecretClient struct {
	output *secretsmanager.GetSecretValueOutput
	err    error
	input  *secretsmanager.GetSecretValueInput
}

func (client *fakeTelegramSecretClient) GetSecretValue(
	_ context.Context,
	input *secretsmanager.GetSecretValueInput,
	_ ...func(*secretsmanager.Options),
) (*secretsmanager.GetSecretValueOutput, error) {
	client.input = input
	return client.output, client.err
}

func TestLoadTelegramBotTokenUsesCurrentSecretsManagerValue(t *testing.T) {
	client := &fakeTelegramSecretClient{output: &secretsmanager.GetSecretValueOutput{
		SecretString: aws.String("123456:bot-token"),
	}}
	token, err := loadTelegramBotToken(context.Background(), client, "arn:secret")
	if err != nil || token != "123456:bot-token" || aws.ToString(client.input.SecretId) != "arn:secret" {
		t.Fatalf("token=%q input=%#v err=%v", token, client.input, err)
	}
}

func TestLoadTelegramBotTokenRejectsEmptyOrMalformedSecretWithoutEchoingIt(t *testing.T) {
	for _, output := range []*secretsmanager.GetSecretValueOutput{
		nil,
		{},
		{SecretString: aws.String("token\nleak")},
	} {
		client := &fakeTelegramSecretClient{output: output}
		_, err := loadTelegramBotToken(context.Background(), client, "arn:secret")
		if err == nil || strings.Contains(err.Error(), "token\nleak") {
			t.Fatalf("error = %v", err)
		}
	}
	private := errors.New("private provider failure")
	_, err := loadTelegramBotToken(context.Background(), &fakeTelegramSecretClient{err: private}, "arn:secret")
	if !errors.Is(err, private) {
		t.Fatalf("provider error = %v", err)
	}
}
