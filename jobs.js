const express = require('express');
const mongoose = require('mongoose');

const router = express.Router();

const jobSchema = new mongoose.Schema({
    source: String,
    skill: String,
    jobKey: String,
    title: String,
    company: String,
    location: String,
    description: String,
    postedDate: String,
    url: String,
    scrapedAt: Date,
}, { collection: 'jobs' });

const Job = mongoose.models.Job || mongoose.model('Job', jobSchema);

// GET /api/jobs — list with optional filters and pagination
router.get('/api/jobs', async (req, res) => {
    try {
        const { skill, source, location, search, page = 1, limit = 12 } = req.query;
        const query = {};
        if (skill) query.skill = { $regex: skill, $options: 'i' };
        if (source) query.source = { $regex: source, $options: 'i' };
        if (location) query.location = { $regex: location, $options: 'i' };
        if (search) {
            query.$or = [
                { title: { $regex: search, $options: 'i' } },
                { company: { $regex: search, $options: 'i' } },
                { description: { $regex: search, $options: 'i' } },
            ];
        }

        const total = await Job.countDocuments(query);
        const jobs = await Job.find(query)
            .sort({ scrapedAt: -1 })
            .skip((Number(page) - 1) * Number(limit))
            .limit(Number(limit));

        res.json({
            jobs,
            total,
            page: Number(page),
            limit: Number(limit),
            pages: Math.ceil(total / Number(limit)),
        });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// GET /api/jobs/:id — single job
router.get('/api/jobs/:id', async (req, res) => {
    try {
        const job = await Job.findById(req.params.id);
        if (!job) return res.status(404).json({ error: 'Job not found' });
        res.json(job);
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// GET /api/stats — totals for dashboard
router.get('/api/stats', async (req, res) => {
    try {
        const [total, sources, skills] = await Promise.all([
            Job.countDocuments(),
            Job.distinct('source'),
            Job.distinct('skill'),
        ]);
        res.json({
            total,
            sources: sources.filter(Boolean),
            skills: skills.filter(Boolean),
        });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// GET /api/filters — distinct values for filter dropdowns
router.get('/api/filters', async (req, res) => {
    try {
        const [skills, sources, locations] = await Promise.all([
            Job.distinct('skill'),
            Job.distinct('source'),
            Job.distinct('location'),
        ]);
        res.json({
            skills: skills.filter(Boolean).sort(),
            sources: sources.filter(Boolean).sort(),
            locations: locations.filter(Boolean).sort(),
        });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

module.exports = router;
