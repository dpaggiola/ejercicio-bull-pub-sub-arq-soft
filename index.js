const express = require("express");
const redis = require("redis");
const Queue = require("bull");

//Create a new Bull queue
const redisClient = redis.createClient();
const myQueue = new Queue("myQueue", { redis: redisClient });

const subscribers = {
    // "eventId": []
};

//Bull job processor
myQueue.process(async (job, done) => {
    console.log(`Processing job ${job.id}`);
    // Do some async work here
    // ...
    console.log(`Job ${job.id} completed`)

    //Pub logic
    if (subscribers[job.id]) {
        try {
            //First way
            redisClient.connect();
            redisClient.publish(job.id, job.data);
            //Seconday way
            // subscribers[job.id].forEach((subscriber) => {
            //   subscriber.response.json({
            //     message: "Task accomplished",
            //     data: subscriber.data,
            //   });
            // });
        } catch (error) {
            done(error);
            console.log("Error", error);
        } finally {
            redisClient.quit();
        }
        done();
    }
});

//Create an Express app
const app = express();

app.use(express.json());

app.post("/jobs", async (req, res) => {
    const { data } = req.body;
    if (!data) {
        res.status(400).send("Job data is required");
        return;
    }
    try {
        const job = await myQueue.add(data, { delay: 3000 });
        //Sub logic
        await redisClient.connect();
        await redisClient.subscribe(job.id, (message) => {
            console.log(message);
        });

        if (!subscribers[job.id]) {
            subscribers[job.id] = [];
        }
        subscribers[job.id].push({
            id: job.id,
            data,
            response: res,
        });
        //Sub logic

        //Commented given an error
        res.json({ id: job.id });
    } catch (error) {
        console.log("Error", error);
    } finally {
        redisClient.quit();
    }
});

app.get("/jobs/:id", async (req, res) => {
    const { id } = req.params;
    const job = await myQueue.getJob(id);
    if (!job) {
        return res.status(404).json({ error: "Job not found" });
    }
    const state = await job.getState();
    res.json({ state, job});
});

app.listen(3000, () => {
    console.log("Server listening on port 3000");
});