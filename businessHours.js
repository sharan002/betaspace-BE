// ─── Office-hours-aware duration helpers (IST, Mon–Sat, 09:30–18:30) ──────
// India has no DST, so IST is always a fixed UTC+5:30 offset — no timezone
// library needed. All calendar-day/weekday/time-of-day checks below are done
// by shifting the UTC timestamp by the IST offset and reading UTC getters,
// which yields IST "wall clock" fields regardless of the server's local TZ.

const IST_OFFSET_MS = 5.5 * 60 * 60 * 1000;
const OFFICE_START_MIN = 9 * 60 + 30;  // 09:30
const OFFICE_END_MIN = 18 * 60 + 30;   // 18:30
const DAY_MS = 24 * 60 * 60 * 1000;
const MAX_DAY_ITERATIONS = 3660; // ~10 years safety cap

function toIST(date) {
  return new Date(date.getTime() + IST_OFFSET_MS);
}

function istDayStartUTCms(istShiftedDate) {
  return Date.UTC(istShiftedDate.getUTCFullYear(), istShiftedDate.getUTCMonth(), istShiftedDate.getUTCDate());
}

function isWorkingWeekday(weekday) {
  return weekday >= 1 && weekday <= 6; // Monday(1)..Saturday(6), Sunday(0) excluded
}

// Office window [09:30, 18:30) IST for the IST calendar day containing `date`,
// returned as real UTC Date instants, plus that day's IST weekday.
function officeWindowForDay(date) {
  const ist = toIST(date);
  const dayStartIST_ms = istDayStartUTCms(ist);
  return {
    start: new Date(dayStartIST_ms + OFFICE_START_MIN * 60000 - IST_OFFSET_MS),
    end: new Date(dayStartIST_ms + OFFICE_END_MIN * 60000 - IST_OFFSET_MS),
    weekday: ist.getUTCDay(),
  };
}

function isWithinOfficeHours(date) {
  const ist = toIST(date);
  if (!isWorkingWeekday(ist.getUTCDay())) return false;
  const minutesOfDay = ist.getUTCHours() * 60 + ist.getUTCMinutes();
  return minutesOfDay >= OFFICE_START_MIN && minutesOfDay < OFFICE_END_MIN;
}

function totalMinutesBetween(start, end) {
  return (end.getTime() - start.getTime()) / 60000;
}

// Minutes of [start, end) that fall within office hours (Mon–Sat, 09:30–18:30 IST).
function businessMinutesBetween(start, end) {
  if (!(start instanceof Date) || !(end instanceof Date)) return 0;
  if (end <= start) return 0;

  let totalMs = 0;
  let cursor = start;
  let guard = 0;

  while (cursor < end && guard < MAX_DAY_ITERATIONS) {
    const { start: officeStart, end: officeEnd, weekday } = officeWindowForDay(cursor);
    if (isWorkingWeekday(weekday)) {
      const overlapStart = officeStart > start ? officeStart : start;
      const overlapEnd = officeEnd < end ? officeEnd : end;
      if (overlapEnd > overlapStart) {
        totalMs += overlapEnd.getTime() - overlapStart.getTime();
      }
    }
    const ist = toIST(cursor);
    const nextDayStartIST_ms = istDayStartUTCms(ist) + DAY_MS;
    cursor = new Date(nextDayStartIST_ms - IST_OFFSET_MS);
    guard++;
  }

  return totalMs / 60000;
}

// Minutes of [start, end) that fall outside office hours (nights + Sundays).
function offHoursMinutesBetween(start, end) {
  if (!(start instanceof Date) || !(end instanceof Date)) return 0;
  if (end <= start) return 0;
  const total = totalMinutesBetween(start, end);
  const business = businessMinutesBetween(start, end);
  return Math.max(0, total - business);
}

module.exports = {
  isWithinOfficeHours,
  businessMinutesBetween,
  offHoursMinutesBetween,
  totalMinutesBetween,
};
