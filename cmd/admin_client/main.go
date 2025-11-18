package main

import (
	"fmt"
	"log/slog"
	"net/rpc"
	"os"
	"strings"

	"github.com/crdsdev/doc/pkg/models"
)

const (
	defaultGitterAddr = "127.0.0.1:5002"
)

func main() {
	if len(os.Args) < 3 || os.Args[1] != "index" {
		fmt.Println("Usage: admin_client index <org/repo[@version] ...>")
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	client, err := rpc.DialHTTP("tcp", defaultGitterAddr)
	if err != nil {
		logger.Error("failed to connect to gitter server", slog.Any("error", err))
		return
	}

	for _, arg := range os.Args[2:] {
		rest, ok := strings.CutPrefix(arg, "github.com/")
		if !ok {
			logger.Error("invalid target, must start with github.com/", slog.String("target", arg))
			continue
		}

		tagSplit := strings.SplitN(rest, "@", 2)

		tag := ""
		if len(tagSplit) == 2 {
			tag = tagSplit[1]
		}

		repoSplit := strings.SplitN(tagSplit[0], "/", 2)

		info := models.GitterRepo{
			Org:  repoSplit[0],
			Repo: repoSplit[1],
			Tag:  tag,
		}

		reply := ""
		if err := client.Call("Gitter.Index", info, &reply); err != nil {
			logger.Error("failed to index repo", slog.String("target", arg), slog.Any("error", err))
			continue
		}

		logger.Info("indexed repo", slog.String("target", arg), slog.String("reply", reply))
	}
}
