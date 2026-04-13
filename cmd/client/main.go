package main

import (
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"time"

	"com.lwc.raft/internal/core"
	"com.lwc.raft/internal/rpc"
)

const clientTimeout = 10 * time.Second

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	if len(os.Args) < 2 {
		return errors.New("expected subcommand: add, minus, or flip")
	}

	cmd := os.Args[1]
	req, addr, err := parseClientCommand(cmd, os.Args[2:])
	if err != nil {
		return err
	}

	res, finalAddr, err := sendStateActionWithRedirect(addr, req)
	if err != nil {
		return err
	}

	if res.Success {
		fmt.Printf("ok %s\n", finalAddr)
		return nil
	}
	if res.RedirectAddr != "" {
		return fmt.Errorf("request redirected to %s but retry failed", res.RedirectAddr)
	}

	return errors.New("request rejected")
}

func parseClientCommand(cmd string, args []string) (rpc.StateActionReq, string, error) {
	fs := flag.NewFlagSet(cmd, flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	addr := fs.String("addr", "", "target node address")
	delta := fs.Uint("delta", 0, "counter delta")

	if err := fs.Parse(args); err != nil {
		return rpc.StateActionReq{}, "", err
	}

	if *addr == "" {
		return rpc.StateActionReq{}, "", errors.New("missing required -addr")
	}

	switch cmd {
	case "add":
		if *delta == 0 {
			return rpc.StateActionReq{}, "", errors.New("add requires -delta > 0")
		}
		if *delta > 0xffff {
			return rpc.StateActionReq{}, "", errors.New("delta must fit in uint16")
		}
		return rpc.StateActionReq{CounterDelta: uint16(*delta), Action: core.STATE_ADD}, *addr, nil
	case "minus":
		if *delta == 0 {
			return rpc.StateActionReq{}, "", errors.New("minus requires -delta > 0")
		}
		if *delta > 0xffff {
			return rpc.StateActionReq{}, "", errors.New("delta must fit in uint16")
		}
		return rpc.StateActionReq{CounterDelta: uint16(*delta), Action: core.STATE_MINUS}, *addr, nil
	case "flip":
		return rpc.StateActionReq{Action: core.STATE_FLIP}, *addr, nil
	default:
		return rpc.StateActionReq{}, "", fmt.Errorf("unknown subcommand %q", cmd)
	}
}

func sendStateActionWithRedirect(addr string, req rpc.StateActionReq) (rpc.StateActionRes, string, error) {
	res, err := sendStateAction(addr, req)
	if err != nil {
		return rpc.StateActionRes{}, "", err
	}
	if res.Success {
		return res, addr, nil
	}
	if res.RedirectAddr == "" {
		return res, addr, nil
	}

	redirectAddr := res.RedirectAddr
	res, err = sendStateAction(redirectAddr, req)
	if err != nil {
		return rpc.StateActionRes{}, "", err
	}
	if !res.Success && res.RedirectAddr != "" {
		return rpc.StateActionRes{}, "", fmt.Errorf("multiple redirects are not supported: %s", res.RedirectAddr)
	}

	return res, redirectAddr, nil
}

func sendStateAction(addr string, req rpc.StateActionReq) (rpc.StateActionRes, error) {
	conn, err := net.DialTimeout("tcp", addr, clientTimeout)
	if err != nil {
		return rpc.StateActionRes{}, fmt.Errorf("dial %s: %w", addr, err)
	}
	defer conn.Close()

	if err := conn.SetDeadline(time.Now().Add(clientTimeout)); err != nil {
		return rpc.StateActionRes{}, fmt.Errorf("set deadline: %w", err)
	}

	frame := rpc.Frame{
		RPCType:    rpc.RPC_TYPE_STATE_ACTION_REQ,
		RelationId: 1,
		Payload:    req.Encode(),
	}.Encode()

	if err := writeFramed(conn, frame); err != nil {
		return rpc.StateActionRes{}, err
	}

	frameBytes, err := readFramed(conn)
	if err != nil {
		return rpc.StateActionRes{}, err
	}

	resFrame := rpc.DecodeRPCFrame(frameBytes)
	if resFrame.RPCType != rpc.RPC_TYPE_STATE_ACTION_RES {
		return rpc.StateActionRes{}, fmt.Errorf("unexpected rpc type %d", resFrame.RPCType)
	}

	return rpc.DecodeStateActionRes(resFrame.Payload), nil
}

func writeFramed(conn net.Conn, payload []byte) error {
	frameLen := make([]byte, 4)
	binary.BigEndian.PutUint32(frameLen, uint32(len(payload)))

	if err := writeAll(conn, frameLen); err != nil {
		return fmt.Errorf("write frame len: %w", err)
	}
	if err := writeAll(conn, payload); err != nil {
		return fmt.Errorf("write frame payload: %w", err)
	}

	return nil
}

func writeAll(conn net.Conn, payload []byte) error {
	for len(payload) > 0 {
		n, err := conn.Write(payload)
		if err != nil {
			return err
		}
		payload = payload[n:]
	}

	return nil
}

func readFramed(conn net.Conn) ([]byte, error) {
	frameLen := make([]byte, 4)
	if _, err := io.ReadFull(conn, frameLen); err != nil {
		return nil, fmt.Errorf("read frame len: %w", err)
	}

	payload := make([]byte, binary.BigEndian.Uint32(frameLen))
	if _, err := io.ReadFull(conn, payload); err != nil {
		return nil, fmt.Errorf("read frame payload: %w", err)
	}

	return payload, nil
}
