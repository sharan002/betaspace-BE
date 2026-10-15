# Bill Generation & Sending Feature

## Overview
This feature allows you to generate PDF bills for specific installments and send them via email to leads. Each bill includes installment details, amount, due date, and payment information.

---

## Setup & Configuration

### Environment Variables
Add the following variables to your `.env` file:

```env
# Email Configuration (required for sending bills)
EMAIL_SERVICE=gmail          # or your email service provider
EMAIL_USER=your-email@gmail.com
EMAIL_PASSWORD=your-app-password    # For Gmail, use App Password, not account password

# Optional: Default recipient if lead email is not available
DEFAULT_RECIPIENT_EMAIL=admin@yourcompany.com
```

### For Gmail Setup:
1. Enable 2-Factor Authentication on your Gmail account
2. Generate an "App Password" from your Google Account settings
3. Use that App Password in the `EMAIL_PASSWORD` environment variable

---

## API Endpoints

### 1. Generate & Send Bill
**POST** `/leads/:id/generate-bill`

Generate a PDF bill for a specific installment and optionally send it via email.

#### Request Body:
```json
{
  "installmentIndex": 0,           // 0-based index (0 for 1st installment, 1 for 2nd, etc.)
  "sendEmail": true                // Optional: send email. Defaults to true
}
```

#### Response:
```json
{
  "success": true,
  "message": "Bill generated for Installment #1",
  "billGenerated": true,           // Whether PDF was created
  "emailSent": true,               // Whether email was sent
  "installmentNumber": 1,
  "details": {
    "billFile": "bill_[leadId]_installment_1_[timestamp].pdf",
    "emailMessage": "Bill sent successfully to email@example.com"
  }
}
```

#### Example Usage (Frontend - JavaScript):
```javascript
const leadId = "507f1f77bcf86cd799439011";
const installmentIndex = 0;  // For 1st installment

const response = await fetch(`http://localhost:4000/leads/${leadId}/generate-bill`, {
  method: 'POST',
  headers: {
    'Content-Type': 'application/json'
  },
  body: JSON.stringify({
    installmentIndex: installmentIndex,
    sendEmail: true
  })
});

const result = await response.json();
console.log(result);
```

#### Example Usage (cURL):
```bash
curl -X POST http://localhost:4000/leads/507f1f77bcf86cd799439011/generate-bill \
  -H "Content-Type: application/json" \
  -d '{
    "installmentIndex": 0,
    "sendEmail": true
  }'
```

---

### 2. Check Bill Status
**GET** `/leads/:id/bill-status?installmentIndex=0`

Check if a bill has been generated for a specific installment.

#### Query Parameters:
- `installmentIndex` (required): 0-based index of the installment

#### Response:
```json
{
  "success": true,
  "installmentNumber": 1,
  "billGenerated": true,
  "billGeneratedAt": "2024-03-28T10:30:00.000Z",
  "status": "Pending",
  "amount": 5000,
  "dueDate": "2024-04-30T00:00:00.000Z"
}
```

#### Example Usage:
```javascript
const leadId = "507f1f77bcf86cd799439011";
const installmentIndex = 0;

const response = await fetch(
  `http://localhost:4000/leads/${leadId}/bill-status?installmentIndex=${installmentIndex}`
);

const result = await response.json();
console.log(result.billGenerated); // true or false
```

---

## How It Works

1. **Lead Structure**: Each lead can have multiple installments under `fees.installments[]`
2. **PDF Generation**: When you call the generate-bill endpoint, a PDF is created with:
   - Invoice header and ID
   - Lead details (name, phone, course)
   - Installment information (amount, due date, status)
   - Payment terms
   - Invoice date

3. **Email Sending**: The PDF is attached to an email and sent to the lead's email address
4. **Status Tracking**: The `billGenerated` and `billGeneratedAt` fields are updated in the database to track when bills were sent

---

## File Structure

```
project-root/
├── index.js              # Main server file (updated)
├── Schema.js             # Database schema (updated with bill fields)
├── billUtils.js          # Bill generation utility (NEW)
├── bills/                # Directory where PDFs are saved (created automatically)
│   ├── bill_[id]_installment_1_[timestamp].pdf
│   └── bill_[id]_installment_2_[timestamp].pdf
└── .env                  # Environment variables (needs email config)
```

---

## Error Handling

### Common Errors:

1. **"No email address found for recipient"**
   - Lead doesn't have an email AND DEFAULT_RECIPIENT_EMAIL not set
   - Solution: Add email to lead or set DEFAULT_RECIPIENT_EMAIL in .env

2. **"Installment #X not found"**
   - The installmentIndex doesn't exist for this lead
   - Check if the lead has that many installments

3. **"Email sending failed"**
   - Check EMAIL_USER, EMAIL_PASSWORD, and EMAIL_SERVICE in .env
   - Ensure email credentials are correct
   - Check internet connection

4. **"No fees/installments found"**
   - Lead hasn't had fees set up yet
   - First use the PUT `/leads/:id/fees` endpoint to add fees and installments

---

## Testing the Feature

### Step 1: Set up fees for a lead
```bash
curl -X PUT http://localhost:4000/leads/507f1f77bcf86cd799439011/fees \
  -H "Content-Type: application/json" \
  -d '{
    "billedAmount": 10000,
    "installments": [
      {
        "number": 1,
        "amount": 5000,
        "dueDate": "2024-04-30",
        "status": "Pending"
      },
      {
        "number": 2,
        "amount": 5000,
        "dueDate": "2024-05-30",
        "status": "Pending"
      }
    ]
  }'
```

### Step 2: Generate and send bill for 1st installment
```bash
curl -X POST http://localhost:4000/leads/507f1f77bcf86cd799439011/generate-bill \
  -H "Content-Type: application/json" \
  -d '{
    "installmentIndex": 0,
    "sendEmail": true
  }'
```

### Step 3: Check if bill was generated
```bash
curl http://localhost:4000/leads/507f1f77bcf86cd799439011/bill-status?installmentIndex=0
```

---

## Features

✅ Generate professional PDF bills  
✅ Send bills via email with PDF attachment  
✅ Track bill generation status  
✅ Support for multiple installments  
✅ Automatic database tracking  
✅ Error handling and logging  
✅ No breaking changes to existing code  

---

## Future Enhancements

- [ ] Add bill payment receipt tracking
- [ ] Custom bill template support
- [ ] Bulk bill generation for all pending installments
- [ ] SMS bill notifications
- [ ] Bill payment reminders
- [ ] Multi-language invoice support

