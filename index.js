// ===================================
// Email Sender Lambda Function - Direct Gmail API
// Processes email jobs from SQS and sends photo match notifications via Direct Gmail API
// ===================================

import { DynamoDBClient, UpdateItemCommand, GetItemCommand } from '@aws-sdk/client-dynamodb';
import { S3Client, GetObjectCommand } from '@aws-sdk/client-s3'; // (kept if you later inline thumbnails)
import { CloudWatchClient, PutMetricDataCommand } from '@aws-sdk/client-cloudwatch';
import https from 'https';
import querystring from 'querystring';

// Configuration
const CONFIG = {
  AWS_REGION: process.env.AWS_REGION || 'ap-south-1',

  // Gmail OAuth Configuration
  GMAIL_USER: process.env.GMAIL_USER,
  GMAIL_CLIENT_ID: process.env.GMAIL_CLIENT_ID,
  GMAIL_CLIENT_SECRET: process.env.GMAIL_CLIENT_SECRET,
  GMAIL_REFRESH_TOKEN: process.env.GMAIL_REFRESH_TOKEN,

  // Email settings
  FROM_EMAIL: process.env.FROM_EMAIL || process.env.GMAIL_USER,
  FROM_NAME: process.env.FROM_NAME || 'Hapzea Photo Sharing',
  REPLY_TO_EMAIL: process.env.REPLY_TO_EMAIL || 'support@hapzea.com',

  // Feature flags
  ENABLE_METRICS: process.env.ENABLE_METRICS !== 'false',
  ENABLE_DEBUG_LOGGING: process.env.ENABLE_DEBUG_LOGGING === 'true',

  // Email content settings
  MAX_PHOTOS_IN_EMAIL: parseInt(process.env.MAX_PHOTOS_IN_EMAIL || '6'),
  SUPPORT_EMAIL: process.env.SUPPORT_EMAIL || 'support@hapzea.com',
  COMPANY_WEBSITE: process.env.COMPANY_WEBSITE || 'https://hapzea.com',

  // Error handling
  MAX_RETRIES: parseInt(process.env.MAX_RETRIES || '3'),
};

// Initialize AWS clients
const dynamoClient = new DynamoDBClient({ region: CONFIG.AWS_REGION });
const s3Client = new S3Client({ region: CONFIG.AWS_REGION });
const cloudwatchClient = new CloudWatchClient({ region: CONFIG.AWS_REGION });

// ===================================
// Main Lambda Handler
// ===================================

export const handler = async (event, context) => {
  console.log('📧 Email Sender Lambda started');
  console.log(`📊 Processing ${event.Records.length} SQS messages`);

  if (CONFIG.ENABLE_DEBUG_LOGGING) {
    console.log('📋 SQS Event:', JSON.stringify(event, null, 2));
  }

  const metrics = {
    totalMessages: event.Records.length,
    processedMessages: 0,
    emailsSent: 0,
    failedMessages: 0,
    duplicatesSkipped: 0,
  };

  const results = [];

  for (const record of event.Records) {
    try {
      console.log(`\n📨 Processing message: ${record.messageId}`);

      const result = await processEmailMessage(record);
      results.push(result);

      metrics.processedMessages++;
      if (result.status === 'sent') {
        metrics.emailsSent++;
      } else if (result.status === 'skipped') {
        metrics.duplicatesSkipped++;
      } else if (result.status === 'failed') {
        metrics.failedMessages++;
      }
    } catch (error) {
      console.error(`❌ Error processing message ${record.messageId}:`, error);
      metrics.failedMessages++;

      results.push({
        messageId: record.messageId,
        status: 'failed',
        error: error.message,
      });
    }
  }

  if (CONFIG.ENABLE_METRICS) {
    await publishMetrics(metrics);
  }

  console.log('\n📊 Email Processing Summary:');
  console.log(`   Total Messages: ${metrics.totalMessages}`);
  console.log(`   Processed Successfully: ${metrics.processedMessages}`);
  console.log(`   Emails Sent: ${metrics.emailsSent}`);
  console.log(`   Duplicates Skipped: ${metrics.duplicatesSkipped}`);
  console.log(`   Failed Messages: ${metrics.failedMessages}`);

  const failedMessages = results
    .filter((r) => r.status === 'failed')
    .map((r) => ({ itemIdentifier: r.messageId }));

  return {
    batchItemFailures: failedMessages,
    summary: metrics,
    processedAt: new Date().toISOString(),
  };
};

// ===================================
// Email Message Processing
// ===================================

async function processEmailMessage(sqsRecord) {
  let emailJob = null;

  try {
    const messageBody = JSON.parse(sqsRecord.body);
    emailJob = messageBody.payload;

    console.log(`   📋 Processing email job for guest: ${emailJob.guestId}`);
    console.log(`   📊 Total matches: ${emailJob.matchInfo.totalMatches}`);

    if (CONFIG.ENABLE_DEBUG_LOGGING) {
      console.log('   📋 Email job details:', JSON.stringify(emailJob, null, 2));
    }

    // Duplicate detection
    const emailSent = await checkIfEmailSent(emailJob.eventId, emailJob.guestId);
    if (emailSent) {
      console.log('   ⏭️  Email already sent, skipping duplicate');
      return {
        messageId: sqsRecord.messageId,
        status: 'skipped',
        reason: 'Email already sent',
        eventId: emailJob.eventId,
        guestId: emailJob.guestId,
      };
    }

    // Validate job
    const validation = validateEmailJob(emailJob);
    if (!validation.isValid) {
      console.log(`   ⚠️  Invalid email job: ${validation.reason}`);
      await updateEmailStatus(emailJob.eventId, emailJob.guestId, 'failed', validation.reason);
      return {
        messageId: sqsRecord.messageId,
        status: 'failed',
        reason: validation.reason,
        eventId: emailJob.eventId,
        guestId: emailJob.guestId,
      };
    }

    // Build content (now safe even if no top photos)
    console.log('   🎨 Generating email content...');
    const emailContent = await generateEmailContent(emailJob);

    // Send via Gmail API
    console.log(`   📧 Sending email to: ${emailJob.guestInfo.email}`);
    const sendResult = await sendEmailViaGmailAPI(emailJob.guestInfo.email, emailContent, emailJob);

    if (sendResult.success) {
      console.log(`   ✅ Email sent successfully: ${sendResult.messageId}`);

      // Mark sent + delivered to stop stream re-triggers
      await updateEmailStatus(
        emailJob.eventId,
        emailJob.guestId,
        'sent',
        null,
        sendResult.messageId
      );

      return {
        messageId: sqsRecord.messageId,
        status: 'sent',
        eventId: emailJob.eventId,
        guestId: emailJob.guestId,
        emailMessageId: sendResult.messageId,
        recipientEmail: emailJob.guestInfo.email,
      };
    } else {
      console.error(`   ❌ Email sending failed: ${sendResult.error}`);
      await updateEmailStatus(emailJob.eventId, emailJob.guestId, 'failed', sendResult.error);
      throw new Error(`Email sending failed: ${sendResult.error}`);
    }
  } catch (error) {
    console.error('   💥 Error in processEmailMessage:', error);

    // Record failure to prevent “processing” limbo if the job was parsed
    try {
      if (emailJob?.eventId && emailJob?.guestId) {
        await updateEmailStatus(
          emailJob.eventId,
          emailJob.guestId,
          'failed',
          `template_or_send_error: ${error.message}`
        );
      }
    } catch (e2) {
      console.error('   (secondary) failed to update email status after error:', e2);
    }

    throw error;
  }
}

// ===================================
// Email Job Validation
// ===================================

function validateEmailJob(emailJob) {
  if (!emailJob) return { isValid: false, reason: 'Missing email job data' };
  if (!emailJob.eventId || !emailJob.guestId) {
    return { isValid: false, reason: 'Missing eventId or guestId' };
  }
  if (!emailJob.guestInfo) return { isValid: false, reason: 'Missing guest information' };
  if (!emailJob.guestInfo.email || !isValidEmail(emailJob.guestInfo.email)) {
    return { isValid: false, reason: 'Missing or invalid guest email' };
  }
  if (!emailJob.guestInfo.name) return { isValid: false, reason: 'Missing guest name' };
  if (!emailJob.matchInfo || typeof emailJob.matchInfo.totalMatches !== 'number') {
    return { isValid: false, reason: 'Missing or invalid match information' };
  }
  if (emailJob.matchInfo.totalMatches <= 0) {
    return { isValid: false, reason: 'No matches to notify about' };
  }
  if (!emailJob.emailMetadata || !emailJob.emailMetadata.galleryUrl) {
    return { isValid: false, reason: 'Missing gallery URL' };
  }
  return { isValid: true };
}

function isValidEmail(email) {
  const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
  return emailRegex.test(email);
}

// ===================================
// Email Content Generation
// ===================================

async function generateEmailContent(emailJob) {
  try {
    const templateVars = {
      guestName: emailJob.guestInfo.name,
      guestEmail: emailJob.guestInfo.email,
      eventName: emailJob.emailMetadata.eventName,
      businessName: emailJob.emailMetadata.businessName || 'Hapzea',
      businessLogo: emailJob.emailMetadata.businessLogo || null,
      businessDescription: emailJob.emailMetadata.businessDescription || null,
      businessWebsite: emailJob.emailMetadata.businessWebsite || null,
      businessPhone: emailJob.emailMetadata.businessPhone || null,
      businessEmail: emailJob.emailMetadata.businessEmail || null,
      businessAddress: emailJob.emailMetadata.businessAddress || null,
      socialLinks: emailJob.emailMetadata.socialLinks || {},
      photoCount: emailJob.matchInfo.totalMatches,
      galleryUrl:
        `${emailJob.clientDomain || 'https://hapzea.com'}` +
        `/gallery?eventId=${emailJob.eventId}&guestId=${emailJob.guestId}`,
      bestSimilarity: Math.round((emailJob.matchInfo.bestSimilarity ?? 0) * 100),
      averageSimilarity: Math.round((emailJob.matchInfo.averageSimilarity ?? 0) * 100),
      newMatches: emailJob.matchInfo.newMatches || emailJob.matchInfo.totalMatches,
      supportEmail: CONFIG.SUPPORT_EMAIL,
      companyWebsite: CONFIG.COMPANY_WEBSITE,
      currentYear: new Date().getFullYear(),
      eventId: emailJob.eventId,
      guestId: emailJob.guestId,
      processedDate: new Date(emailJob.emailMetadata.processedAt || Date.now()).toLocaleDateString(),

      // ✅ SAFE, GUARDED topPhotos for the template
      topPhotos: (emailJob.matchInfo?.topMatches || [])
        .slice(0, CONFIG.MAX_PHOTOS_IN_EMAIL)
        .map((m, i) => ({
          url: m.imageUrl,
          alt: `Matched photo ${i + 1}`,
          similarity: Math.round((m.similarity ?? 0) * 100),
        })),
    };

    const emailContent = {
      subject: generateEmailSubject(templateVars),
      html: getEmailHTML(templateVars),
      text: generateTextVersion(templateVars),
    };

    return emailContent;
  } catch (error) {
    console.error('Error generating email content:', error);
    throw new Error(`Failed to generate email content: ${error.message}`);
  }
}

function generateEmailSubject(vars) {
  return vars.photoCount === 1
    ? `Great news ${vars.guestName}! We found your photo from the event`
    : `Amazing! We found ${vars.photoCount} photos of you from the event`;
}

function getEmailHTML(vars) {
  // ✅ guard against undefined
  const photos = Array.isArray(vars.topPhotos) ? vars.topPhotos : [];
  const photosGrid = photos
    .map(
      (photo, index) => `
        <div style="position: relative; overflow: hidden; border-radius: 16px; box-shadow: 0 10px 30px rgba(0,0,0,0.1); transition: transform 0.3s ease;">
          <img src="${photo.url}" alt="${photo.alt}" style="width: 100%; height: 200px; object-fit: cover; display: block;" />
          <div style="position: absolute; bottom: 0; left: 0; right: 0; background: linear-gradient(to top, rgba(0,0,0,0.7), transparent); padding: 12px; color: white;">
            <span style="font-size: 12px; font-weight: 600;">${photo.similarity}% Match</span>
          </div>
        </div>`
    )
    .join('');

  return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
  <title>Your Photos from ${vars.businessName}</title>
  <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
    body, table, td, a { -webkit-text-size-adjust: 100%; -ms-text-size-adjust: 100%; }
    table, td { mso-table-lspace: 0pt; mso-table-rspace: 0pt; }
    img { -ms-interpolation-mode: bicubic; border: 0; outline: none; text-decoration: none; }
    @media only screen and (max-width: 600px) {
      .mobile-hide { display: none !important; }
      .mobile-center { text-align: center !important; }
      .container { width: 100% !important; max-width: 100% !important; }
      .photo-grid { grid-template-columns: 1fr !important; }
      h1 { font-size: 32px !important; }
      .cta-button { width: 100% !important; text-align: center !important; }
    }
  </style>
</head>
<body style="margin:0; padding:0; font-family:'Inter',-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif; background-color:#0a0a0a; color:#ffffff;">
  <div style="display:none; max-height:0; overflow:hidden;">
    ${vars.guestName}, your photos from ${vars.businessName} are ready! We found ${vars.photoCount} amazing shots.
  </div>

  <table border="0" cellpadding="0" cellspacing="0" width="100%" style="background-color:#0a0a0a;">
    <tr>
      <td align="center" style="padding:0;">
        <table class="container" border="0" cellpadding="0" cellspacing="0" width="600" style="max-width:600px; background-color:#0a0a0a;">

          <!-- Header -->
          <tr>
            <td style="padding:40px 32px 20px 32px;">
              <table width="100%" border="0" cellpadding="0" cellspacing="0">
                <tr>
                  <td align="center" style="padding-bottom:30px;">
                    ${
                      vars.businessLogo
                        ? `<img src="${vars.businessLogo}" alt="${vars.businessName}" style="max-height:60px; max-width:200px;"/>`
                        : `<h2 style="margin:0; font-size:28px; font-weight:700; color:#ffffff; letter-spacing:-0.5px;">${vars.businessName}</h2>`
                    }
                  </td>
                </tr>
              </table>

              <!-- Hero -->
              <table width="100%" border="0" cellpadding="0" cellspacing="0">
                <tr>
                  <td align="center" style="padding:20px 0;">
                    <div style="display:inline-block; background:linear-gradient(135deg,#6366f1,#8b5cf6); padding:3px; border-radius:100px;">
                      <div style="background:#0a0a0a; border-radius:100px; padding:12px 24px;">
                        <span style="color:#a78bfa; font-size:14px; font-weight:600; text-transform:uppercase; letter-spacing:1px;">
                          ${vars.photoCount} Photos Found
                        </span>
                      </div>
                    </div>
                  </td>
                </tr>
                <tr>
                  <td align="center" style="padding:10px 0 30px 0;">
                    <h1 style="margin:0; font-size:48px; font-weight:700; color:#ffffff; line-height:1.2; letter-spacing:-1px;">
                      Hey ${vars.guestName},<br/>
                      <span style="background:linear-gradient(135deg,#6366f1,#8b5cf6); -webkit-background-clip:text; -webkit-text-fill-color:transparent; background-clip:text;">
                        Your memories are ready!
                      </span>
                    </h1>
                  </td>
                </tr>
              </table>
            </td>
          </tr>

          <!-- Event Info -->
          <tr>
            <td style="padding:0 32px;">
              <table width="100%" border="0" cellpadding="0" cellspacing="0" style="background:linear-gradient(135deg,#1a1a1a,#262626); border-radius:24px; overflow:hidden;">
                <tr>
                  <td style="padding:32px;">
                    <table width="100%" border="0" cellpadding="0" cellspacing="0">
                      <tr>
                        <td>
                          <p style="margin:0 0 8px 0; color:#a1a1aa; font-size:14px; text-transform:uppercase; letter-spacing:0.5px;">Event</p>
                          <h3 style="margin:0 0 20px 0; color:#ffffff; font-size:24px; font-weight:600;">${vars.eventName}</h3>
                        </td>
                      </tr>
                      <tr>
                        <td>
                          <table width="100%" border="0" cellpadding="0" cellspacing="0">
                            <tr>
                              <td width="33%" style="text-align:center; padding:0 10px;">
                                <div style="color:#6366f1; font-size:32px; font-weight:700; margin-bottom:4px;">${vars.photoCount}</div>
                                <div style="color:#a1a1aa; font-size:12px; text-transform:uppercase;">Total Photos</div>
                              </td>
                              <td width="33%" style="text-align:center; padding:0 10px; border-left:1px solid #333; border-right:1px solid #333;">
                                <div style="color:#8b5cf6; font-size:32px; font-weight:700; margin-bottom:4px;">${vars.bestSimilarity}%</div>
                                <div style="color:#a1a1aa; font-size:12px; text-transform:uppercase;">Best Match</div>
                              </td>
                              <td width="33%" style="text-align:center; padding:0 10px;">
                                <div style="color:#a78bfa; font-size:32px; font-weight:700; margin-bottom:4px;">${vars.processedDate}</div>
                                <div style="color:#a1a1aa; font-size:12px; text-transform:uppercase;">Date</div>
                              </td>
                            </tr>
                          </table>
                        </td>
                      </tr>
                    </table>
                  </td>
                </tr>
              </table>
            </td>
          </tr>

          <!-- Photo Preview (shows if we have photos) -->
          ${
            photos.length
              ? `<tr>
                   <td style="padding:40px 32px;">
                     <h3 style="margin:0 0 24px 0; color:#ffffff; font-size:20px; font-weight:600; text-align:center;">Preview of Your Photos</h3>
                     <div class="photo-grid" style="display:grid; grid-template-columns:repeat(auto-fit, minmax(180px, 1fr)); gap:16px;">
                       ${photosGrid}
                     </div>
                   </td>
                 </tr>`
              : ''
          }

          <!-- CTA -->
          <tr>
            <td style="padding:0 32px 40px 32px;">
              <table width="100%" border="0" cellpadding="0" cellspacing="0">
                <tr>
                  <td align="center">
                    <a href="${vars.galleryUrl}" class="cta-button" style="display:inline-block; background:linear-gradient(135deg,#6366f1,#8b5cf6); color:#ffffff; text-decoration:none; padding:18px 48px; border-radius:100px; font-size:16px; font-weight:600; letter-spacing:0.5px; box-shadow:0 20px 40px rgba(99,102,241,0.3); transition:all 0.3s ease;">
                      View Your Complete Gallery →
                    </a>
                    <p style="margin:16px 0 0 0; color:#71717a; font-size:14px;">Download all your high-resolution photos</p>
                  </td>
                </tr>
              </table>
            </td>
          </tr>

          <!-- Business Info -->
          <tr>
            <td style="padding:40px 32px; border-top:1px solid #262626;">
              <table width="100%" border="0" cellpadding="0" cellspacing="0">
                <tr>
                  <td align="center">
                    <h3 style="margin:0 0 20px 0; color:#ffffff; font-size:18px; font-weight:600;">
                      Captured by ${vars.businessName}
                    </h3>
                    ${
                      vars.businessDescription
                        ? `<p style="margin:0 0 20px 0; color:#a1a1aa; font-size:14px; line-height:1.6; max-width:400px;">${vars.businessDescription}</p>`
                        : ''
                    }

                    <table border="0" cellpadding="0" cellspacing="0" style="margin:20px auto;">
                      <tr>
                        ${
                          vars.businessPhone
                            ? `<td style="padding:0 20px;"><a href="tel:${vars.businessPhone}" style="color:#6366f1; text-decoration:none; font-size:14px;">📞 ${vars.businessPhone}</a></td>`
                            : ''
                        }
                        ${
                          vars.businessEmail
                            ? `<td style="padding:0 20px;"><a href="mailto:${vars.businessEmail}" style="color:#6366f1; text-decoration:none; font-size:14px;">✉️ ${vars.businessEmail}</a></td>`
                            : ''
                        }
                      </tr>
                    </table>

                    ${
                      (vars.socialLinks?.facebook ||
                        vars.socialLinks?.instagram ||
                        vars.socialLinks?.twitter)
                        ? `<table border="0" cellpadding="0" cellspacing="0" style="margin:24px auto;">
                             <tr>
                               ${
                                 vars.socialLinks.facebook
                                   ? `<td style="padding:0 8px;"><a href="${vars.socialLinks.facebook}" style="display:inline-block;"><img src="https://img.icons8.com/ios-filled/50/6366f1/facebook-new.png" alt="Facebook" width="32" height="32"/></a></td>`
                                   : ''
                               }
                               ${
                                 vars.socialLinks.instagram
                                   ? `<td style="padding:0 8px;"><a href="${vars.socialLinks.instagram}" style="display:inline-block;"><img src="https://img.icons8.com/ios-filled/50/6366f1/instagram-new.png" alt="Instagram" width="32" height="32"/></a></td>`
                                   : ''
                               }
                               ${
                                 vars.socialLinks.twitter
                                   ? `<td style="padding:0 8px;"><a href="${vars.socialLinks.twitter}" style="display:inline-block;"><img src="https://img.icons8.com/ios-filled/50/6366f1/twitter.png" alt="Twitter" width="32" height="32"/></a></td>`
                                   : ''
                               }
                             </tr>
                           </table>`
                        : ''
                    }

                    ${
                      vars.businessWebsite
                        ? `<a href="${vars.businessWebsite}" style="display:inline-block; margin-top:16px; color:#6366f1; text-decoration:none; font-size:14px; font-weight:600;">Visit Our Website →</a>`
                        : ''
                    }
                  </td>
                </tr>
              </table>
            </td>
          </tr>

          <!-- Footer -->
          <tr>
            <td style="padding:32px; background-color:#050505; text-align:center;">
              <p style="margin:0 0 8px 0; color:#71717a; font-size:12px;">
                This email was sent to ${vars.guestEmail} because you registered for ${vars.eventName}.
              </p>
              <p style="margin:0; color:#52525b; font-size:11px;">
                © ${vars.currentYear} ${vars.businessName}. All rights reserved.
                ${vars.businessAddress ? `<br/>${vars.businessAddress}` : ''}
              </p>
              <p style="margin:16px 0 0 0; color:#3f3f46; font-size:10px;">
                Powered by <a href="https://hapzea.com" style="color:#3f3f46; text-decoration:none;">Hapzea</a>
              </p>
            </td>
          </tr>

        </table>
      </td>
    </tr>
  </table>
</body>
</html>`;
}

function generateTextVersion(vars) {
  return `
Hi ${vars.guestName}!

Great news! We found ${vars.photoCount} photo${vars.photoCount > 1 ? 's' : ''} of you from ${vars.eventName}.

View your personal photo gallery: ${vars.galleryUrl}

Match Statistics:
- Total photos found: ${vars.photoCount}
- Best match accuracy: ${vars.bestSimilarity}%
- Average match accuracy: ${vars.averageSimilarity}%

If you have any questions, please contact us at ${vars.supportEmail}.

Best regards,
The ${vars.companyWebsite} Team

---
Event ID: ${vars.eventId}
Processed: ${vars.processedDate}
`.trim();
}

// ===================================
// Gmail OAuth and API Functions
// ===================================

async function getGmailAccessToken() {
  return new Promise((resolve, reject) => {
    const postData = querystring.stringify({
      client_id: CONFIG.GMAIL_CLIENT_ID,
      client_secret: CONFIG.GMAIL_CLIENT_SECRET,
      refresh_token: CONFIG.GMAIL_REFRESH_TOKEN,
      grant_type: 'refresh_token',
    });

    const options = {
      hostname: 'oauth2.googleapis.com',
      port: 443,
      path: '/token',
      method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Content-Length': Buffer.byteLength(postData),
      },
    };

    const req = https.request(options, (res) => {
      let data = '';

      res.on('data', (chunk) => {
        data += chunk;
      });

      res.on('end', () => {
        try {
          const response = JSON.parse(data);

          if (res.statusCode === 200) {
            resolve({
              success: true,
              access_token: response.access_token,
              expires_in: response.expires_in,
            });
          } else {
            reject(new Error(`OAuth error: ${response.error} - ${response.error_description}`));
          }
        } catch (parseError) {
          reject(new Error(`Failed to parse OAuth response: ${data}`));
        }
      });
    });

    req.on('error', (error) => {
      reject(new Error(`OAuth request failed: ${error.message}`));
    });

    req.write(postData);
    req.end();
  });
}

async function sendEmailViaGmailAPI(recipientEmail, emailContent, emailJob) {
  try {
    console.log(`   📧 Sending email via Gmail API to: ${recipientEmail}`);

    const tokenResult = await getGmailAccessToken();
    if (!tokenResult.success) {
      throw new Error('Failed to get access token');
    }

    const rfc2822Email = createRFC2822Email(recipientEmail, emailContent, emailJob);
    const result = await sendViaGmailAPI(tokenResult.access_token, rfc2822Email);

    if (result.success) {
      console.log(`   ✅ Email sent successfully via Gmail API`);
      return {
        success: true,
        messageId: result.messageId,
        recipient: recipientEmail,
        service: 'gmail-api',
        threadId: result.threadId,
      };
    } else {
      throw new Error(`Gmail API error: ${result.error}`);
    }
  } catch (error) {
    console.error('   ❌ Gmail API sending error:', error);
    return { success: false, error: error.message, service: 'gmail-api' };
  }
}

function createRFC2822Email(recipientEmail, emailContent, emailJob) {
  const fromEmail = CONFIG.FROM_EMAIL;
  const fromName = CONFIG.FROM_NAME;
  const replyTo = CONFIG.REPLY_TO_EMAIL;
  const date = new Date().toUTCString();
  const messageId = `<${Date.now()}.${Math.random().toString(36)}@hapzea.com>`;

  // IMPORTANT: Gmail requires the From to be the authenticated user or a verified alias
  // If FROM_EMAIL != GMAIL_USER, make sure the alias is verified in Gmail settings.
  const headers = [
    `To: ${recipientEmail}`,
    `From: ${fromName} <${fromEmail}>`,
    `Reply-To: ${replyTo}`,
    `Subject: ${emailContent.subject}`,
    `Date: ${date}`,
    `Message-ID: ${messageId}`,
    `MIME-Version: 1.0`,
    `Content-Type: multipart/alternative; boundary="boundary123456"`,
    `X-Email-Type: photo_match_notification`,
    `X-Event-ID: ${emailJob.eventId}`,
    `X-Guest-ID: ${emailJob.guestId}`,
    `X-Photo-Count: ${emailJob.matchInfo.totalMatches}`,
    `X-Mailer: Hapzea-FaceSearch-v3.0`,
  ];

  return [
    ...headers,
    ``,
    `--boundary123456`,
    `Content-Type: text/plain; charset=UTF-8`,
    `Content-Transfer-Encoding: 7bit`,
    ``,
    emailContent.text,
    ``,
    `--boundary123456`,
    `Content-Type: text/html; charset=UTF-8`,
    `Content-Transfer-Encoding: 7bit`,
    ``,
    emailContent.html,
    ``,
    `--boundary123456--`,
  ].join('\r\n');
}

async function sendViaGmailAPI(accessToken, rfc2822Email) {
  return new Promise((resolve) => {
    const encodedEmail = Buffer.from(rfc2822Email)
      .toString('base64')
      .replace(/\+/g, '-')
      .replace(/\//g, '_')
      .replace(/=+$/, '');

    const postData = JSON.stringify({ raw: encodedEmail });

    const options = {
      hostname: 'gmail.googleapis.com',
      port: 443,
      path: '/gmail/v1/users/me/messages/send',
      method: 'POST',
      headers: {
        Authorization: `Bearer ${accessToken}`,
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(postData),
      },
    };

    const req = https.request(options, (res) => {
      let data = '';

      res.on('data', (chunk) => {
        data += chunk;
      });

      res.on('end', () => {
        try {
          const response = JSON.parse(data);

          if (res.statusCode === 200) {
            resolve({ success: true, messageId: response.id, threadId: response.threadId });
          } else {
            resolve({
              success: false,
              error: response.error ? response.error.message : `HTTP ${res.statusCode}`,
              statusCode: res.statusCode,
              response: data,
            });
          }
        } catch (parseError) {
          resolve({ success: false, error: `JSON parse error: ${parseError.message}`, response: data });
        }
      });
    });

    req.on('error', (error) => {
      resolve({ success: false, error: `Request error: ${error.message}` });
    });

    req.write(postData);
    req.end();
  });
}

// ===================================
// DynamoDB Operations - Updated for new email column + delivery_status
// ===================================

async function checkIfEmailSent(eventId, guestId) {
  try {
    const command = new GetItemCommand({
      TableName: 'face_match_results',
      Key: {
        eventId: { S: eventId },
        guestId: { S: guestId },
      },
      ProjectionExpression: 'email_status, email_sent',
    });

    const result = await dynamoClient.send(command);

    const emailStatus = result.Item?.email_status?.S;
    const emailSent = result.Item?.email_sent?.BOOL;

    return emailStatus === 'sent' || emailSent === true;
  } catch (error) {
    console.error('Error checking email status:', error);
    return false;
  }
}

async function updateEmailStatus(eventId, guestId, status, errorMessage = null, emailMessageId = null) {
  try {
    const updateParts = [
      'email_status = :status',
      'email_sent = :sent',
      'email_updated_at = :timestamp',
    ];
    const eav = {
      ':status': { S: status },
      ':sent': { BOOL: status === 'sent' },
      ':timestamp': { S: new Date().toISOString() },
    };

    if (status === 'sent' && emailMessageId) {
      updateParts.push('email_message_id = :messageId');
      updateParts.push('delivery_status = :delivered');
      updateParts.push('email_delivered_at = :deliveredAt');
      eav[':messageId'] = { S: emailMessageId };
      eav[':delivered'] = { S: 'delivered' };
      eav[':deliveredAt'] = { S: new Date().toISOString() };
    }

    if (status === 'failed' && errorMessage) {
      updateParts.push('email_error = :errorMessage');
      eav[':errorMessage'] = { S: errorMessage };
      // Note: We intentionally do NOT change delivery_status here.
      // Stream had set it to 'processing'; SQS will retry this message.
    }

    const command = new UpdateItemCommand({
      TableName: 'face_match_results',
      Key: {
        eventId: { S: eventId },
        guestId: { S: guestId },
      },
      UpdateExpression: `SET ${updateParts.join(', ')}`,
      ExpressionAttributeValues: eav,
      ReturnValues: 'ALL_NEW',
    });

    const result = await dynamoClient.send(command);
    console.log(`   ✅ Email status updated to: ${status}`);
    return result.Attributes;
  } catch (error) {
    console.error('Error updating email status:', error);
    throw error;
  }
}

// ===================================
// Metrics and Monitoring
// ===================================

async function publishMetrics(metrics) {
  try {
    const metricData = [
      {
        MetricName: 'EmailMessagesProcessed',
        Value: metrics.processedMessages,
        Unit: 'Count',
        Dimensions: [{ Name: 'FunctionName', Value: 'email-sender' }],
      },
      {
        MetricName: 'EmailsSent',
        Value: metrics.emailsSent,
        Unit: 'Count',
        Dimensions: [{ Name: 'FunctionName', Value: 'email-sender' }],
      },
      {
        MetricName: 'EmailSendingErrors',
        Value: metrics.failedMessages,
        Unit: 'Count',
        Dimensions: [{ Name: 'FunctionName', Value: 'email-sender' }],
      },
    ];

    const command = new PutMetricDataCommand({
      Namespace: 'FaceSearch/EmailDelivery',
      MetricData: metricData,
    });

    await cloudwatchClient.send(command);
    console.log('📊 Email metrics published to CloudWatch');
  } catch (error) {
    console.error('❌ Failed to publish email metrics:', error);
  }
}

// ===================================
// Configuration Validation
// ===================================

if (!CONFIG.GMAIL_USER || !CONFIG.GMAIL_CLIENT_ID || !CONFIG.GMAIL_CLIENT_SECRET || !CONFIG.GMAIL_REFRESH_TOKEN) {
  console.error('❌ Missing required Gmail OAuth configuration');
  throw new Error('Gmail OAuth configuration is incomplete');
}

console.log('🔧 Email Sender Configuration:');
console.log(`  AWS Region: ${CONFIG.AWS_REGION}`);
console.log(`  Gmail User: ${CONFIG.GMAIL_USER}`);
console.log(`  From Name: ${CONFIG.FROM_NAME}`);
console.log(`  Reply To: ${CONFIG.REPLY_TO_EMAIL}`);
console.log(`  Max Photos in Email: ${CONFIG.MAX_PHOTOS_IN_EMAIL}`);
console.log(`  Metrics Enabled: ${CONFIG.ENABLE_METRICS}`);
console.log(`  Debug Logging: ${CONFIG.ENABLE_DEBUG_LOGGING}`);
console.log(`  Support Email: ${CONFIG.SUPPORT_EMAIL}`);
console.log(`  Company Website: ${CONFIG.COMPANY_WEBSITE}`);
console.log(`  Email Method: Direct Gmail API`);
