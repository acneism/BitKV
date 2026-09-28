package server

import (
	"errors"
	"fmt"

	"github.com/acneism/casketdb/internal/replica"
)

func cmdRaft(s *Server, c *client, args [][]byte) reply {
	rep := s.cfg.Replica
	if rep == nil {
		return errorReply("ERR RAFT needs a cluster, start the node with -raft-id")
	}
	var err error
	switch sub := upper(args[1]); {
	case sub == "TRANSFER" && len(args) <= 3:
		to := ""
		if len(args) == 3 {
			to = string(args[2])
		}
		err = rep.TransferLeadership(to)
	default:
		return unknownSubcommand(args)
	}
	if err != nil {
		return raftError(rep, err)
	}
	return okReply
}

func raftError(rep *replica.Node, err error) errorReply {
	if !errors.Is(err, replica.ErrNotLeader) {
		return errorReply("ERR " + err.Error())
	}
	st := rep.Status()
	if st.LeaderID == "" {
		return errorReply("ERR this node is not the leader and no leader is known, retry")
	}
	return errorReply(fmt.Sprintf("ERR this node is not the leader, run it on %s (raft address %s)", st.LeaderID, st.LeaderAddr))
}
