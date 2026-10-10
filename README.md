[README.md](https://github.com/user-attachments/files/33281035/README.md)
# The Ripple Effect — Live Buzzer

A real-time host-and-team buzzer built with Node.js, Express and Socket.IO.

## Run locally
1. Install Node.js 18 or newer.
2. In this folder, run `npm install`.
3. Run `npm start`.
4. Open `http://localhost:3000` on the host laptop. Teams on the same network can join using the laptop's LAN IP and room code. For remote teams, deploy the app to a public Node hosting service.

## How it works
- Host creates a room and sets a round timer (0 means no limit; maximum 600 seconds).
- Teams join before the first round goes live.
- Each team can buzz once per round. The host sees the order, local clock timestamp, and elapsed time from the start (for example, `Rewa — 2.3s`).
- **Reset** clears the current round's buzz log and returns to ready/lobby. **Next Round** clears the log and prepares another round; press **Go Live** to start it.
- Host and team sessions can recover after refresh/reconnect for up to five minutes while the room is temporarily disconnected. Room state is in memory, so restarting the Node process clears rooms.

## Notes
- Keep the host tab open during play; brief disconnects are recoverable.
- This project intentionally has no question editor, scores, or leaderboard.
