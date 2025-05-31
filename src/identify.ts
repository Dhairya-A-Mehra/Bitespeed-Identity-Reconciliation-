// src/identify.ts
import { Request, Response } from 'express';
import { supabase } from './supabase';
import { PostgrestError } from '@supabase/supabase-js';

interface Contact {
  id: number;
  email: string | null;
  phoneNumber: string | null;
  linkedId: number | null;
  linkPrecedence: 'primary' | 'secondary';
  createdAt: string; // Supabase typically returns ISO date strings
}

// Define types for insert/update payloads for clarity and type safety.
type ContactInsertData = Omit<Contact, 'id' | 'createdAt'>;
type ContactUpdateData = Partial<Pick<Contact, 'linkPrecedence' | 'linkedId'>>;


export const identify = async (req: Request, res: Response): Promise<Response> => {
  // Critical check for req.body - this needs to be populated by express.json()
  if (!req.body || Object.keys(req.body).length === 0) {
    console.error("FATAL: req.body is undefined or empty. Ensure express.json() middleware is correctly set up AND client is sending 'Content-Type: application/json' with a valid JSON body.");
    console.error("Received req.headers:", JSON.stringify(req.headers));
    return res.status(400).json({ error: 'Request body is missing, empty, or not in JSON format. Please check server configuration and client request (Content-Type header and body format).' });
  }

  // Destructure and ensure email/phoneNumber are string or null
  const { email: rawEmail, phoneNumber: rawPhoneNumber }: { email?: string | null; phoneNumber?: string | null } = req.body;
  const email = rawEmail !== undefined ? (rawEmail === "" ? null : rawEmail) : null;
  const phoneNumber = rawPhoneNumber !== undefined ? (rawPhoneNumber === "" ? null : rawPhoneNumber) : null;


  if (email === null && phoneNumber === null) {
    return res.status(400).json({ error: 'At least email or phoneNumber must be provided and not be empty strings.' });
  }

  try {
    // 1. Find existing contacts by email or phoneNumber
    const orFilterParts: string[] = [];
    if (email !== null) orFilterParts.push(`email.eq.${email}`);
    if (phoneNumber !== null) orFilterParts.push(`phoneNumber.eq.${phoneNumber}`);
    
    const initialOrFilter = orFilterParts.join(',');

    const { data: initialContactsData, error: initialError }: { data: Contact[] | null; error: PostgrestError | null } = await supabase
      .from('Contact')
      .select('*')
      .or(initialOrFilter)
      .order('createdAt', { ascending: true });

    if (initialError) throw initialError;
    let allContactsInvolved: Contact[] = initialContactsData || [];

    let primaryContact: Contact | null = null;
    const newContactInfo = { email, phoneNumber };

    if (allContactsInvolved.length > 0) {
      // Determine the true primary contact ID from the initial set
      const primaryCandidates = allContactsInvolved
        .map(c => (c.linkPrecedence === 'primary' ? c.id : c.linkedId))
        .filter((id): id is number => id !== null);
      
      let rootPrimaryId: number | null = null;
      if (primaryCandidates.length > 0) {
        rootPrimaryId = Math.min(...primaryCandidates);
      } else {
        // No clear primary link, means all are primary or unlinked secondaries
        // Pick the oldest among them to be the reference.
        const oldestContact = [...allContactsInvolved].sort((a,b) => new Date(a.createdAt).getTime() - new Date(b.createdAt).getTime())[0];
        rootPrimaryId = oldestContact.id;
      }

      // Fetch all contacts related to this rootPrimaryId (the entire consolidated group)
      const { data: groupContactsData, error: groupError }: { data: Contact[] | null; error: PostgrestError | null } = await supabase
        .from('Contact')
        .select('*')
        .or(`id.eq.${rootPrimaryId},linkedId.eq.${rootPrimaryId}`)
        .order('createdAt', { ascending: true });

      if (groupError) throw groupError;
      allContactsInvolved = groupContactsData || [];

      // Find the primary contact within this fully resolved group
      primaryContact = allContactsInvolved.find(c => c.id === rootPrimaryId && c.linkPrecedence === 'primary') ||
                       allContactsInvolved.find(c => c.linkPrecedence === 'primary') || // any primary if root isn't
                       null; 
      
      // If no primary contact, the oldest becomes primary (or will be promoted)
      if (!primaryContact && allContactsInvolved.length > 0) {
        allContactsInvolved.sort((a, b) => new Date(a.createdAt).getTime() - new Date(b.createdAt).getTime());
        primaryContact = allContactsInvolved[0];
      }


      // Check if the exact new email/phone pair already exists
      const exactMatchExists = allContactsInvolved.some(
        c => (newContactInfo.email !== null ? c.email === newContactInfo.email : true) && // only check email if new one provided
             (newContactInfo.phoneNumber !== null ? c.phoneNumber === newContactInfo.phoneNumber : true) && // only check phone if new one provided
             // Ensure at least one matches if the other is null in newContactInfo
             ( (newContactInfo.email !== null && c.email === newContactInfo.email) || (newContactInfo.phoneNumber !== null && c.phoneNumber === newContactInfo.phoneNumber) )
      );

      // Logic for creating a new secondary or updating links
      let newRecordNeeded = true;
      if (primaryContact) {
         // Check if the incoming data (email or phone) belongs to any existing contact in the group
         const emailExistsInGroup = newContactInfo.email !== null && allContactsInvolved.some(c => c.email === newContactInfo.email);
         const phoneExistsInGroup = newContactInfo.phoneNumber !== null && allContactsInvolved.some(c => c.phoneNumber === newContactInfo.phoneNumber);

         if (exactMatchExists) {
            newRecordNeeded = false;
         }
         // If only one part of info is new, and the other matches an existing record,
         // and that existing record is not the primary, it might need to be linked.
         // Or if new info links two previously separate primary contacts.
         else if ( (newContactInfo.email && emailExistsInGroup) || (newContactInfo.phoneNumber && phoneExistsInGroup) ) {
             // This is complex: an existing contact might need updating, or a new secondary is needed if the combo is new.
             // For now, if the exact combo doesn't exist, assume a new secondary if new info is provided.
             const specificCombinationExists = allContactsInvolved.some(c => c.email === newContactInfo.email && c.phoneNumber === newContactInfo.phoneNumber);
             if (!specificCombinationExists && (newContactInfo.email || newContactInfo.phoneNumber)) {
                newRecordNeeded = true; // Create new secondary for the new combo
             } else {
                newRecordNeeded = false; // Combination exists or only partial match handled by consolidation
             }
         }
      }


      if (newRecordNeeded && (newContactInfo.email || newContactInfo.phoneNumber)) {
        const newSecondaryData: ContactInsertData = {
          email: newContactInfo.email,
          phoneNumber: newContactInfo.phoneNumber,
          linkedId: primaryContact!.id, // primaryContact should exist if we are here
          linkPrecedence: 'secondary',
        };
        const { data: inserted, error: insertErr } = await supabase
          .from('Contact')
          .insert([newSecondaryData])
          .select();
        if (insertErr) throw insertErr;
        if (inserted) allContactsInvolved.push(...inserted);
      }

      // Consolidate primaries: ensure only one (the oldest) is 'primary'
      const primaries = allContactsInvolved.filter(c => c.linkPrecedence === 'primary');
      if (primaries.length > 1) {
        primaries.sort((a, b) => new Date(a.createdAt).getTime() - new Date(b.createdAt).getTime());
        const truePrimary = primaries[0];
        primaryContact = truePrimary; // Update our primaryContact reference

        for (const p of primaries) {
          if (p.id !== truePrimary.id) {
            const { error: updateErr } = await supabase
              .from('Contact')
              .update({ linkPrecedence: 'secondary', linkedId: truePrimary.id })
              .eq('id', p.id);
            if (updateErr) throw updateErr;
            // Update in-memory record
            const idx = allContactsInvolved.findIndex(c => c.id === p.id);
            if (idx > -1) {
                allContactsInvolved[idx].linkPrecedence = 'secondary';
                allContactsInvolved[idx].linkedId = truePrimary.id;
            }
          }
        }
      } else if (primaries.length === 1) {
        primaryContact = primaries[0];
      }
      // If primaryContact (chosen as oldest) is not 'primary', promote it
      if (primaryContact && primaryContact.linkPrecedence !== 'primary') {
        const { error: promoteError } = await supabase
            .from('Contact')
            .update({ linkPrecedence: 'primary', linkedId: null })
            .eq('id', primaryContact.id);
        if (promoteError) throw promoteError;
        primaryContact.linkPrecedence = 'primary';
        primaryContact.linkedId = null;
      }
      // Ensure all secondaries in the group point to the true primary
      if(primaryContact) {
        for(const contact of allContactsInvolved) {
            if(contact.id !== primaryContact.id && (contact.linkedId !== primaryContact.id || contact.linkPrecedence !== 'secondary')) {
                if(contact.linkPrecedence === 'primary' || contact.linkedId !== primaryContact.id) { // only update if necessary
                    const { error: updateErr } = await supabase
                        .from('Contact')
                        .update({linkPrecedence: 'secondary', linkedId: primaryContact.id})
                        .eq('id', contact.id);
                    if (updateErr) throw updateErr;
                    contact.linkPrecedence = 'secondary';
                    contact.linkedId = primaryContact.id;
                }
            }
        }
      }

    } else {
      // No existing contacts found, create a new primary contact
      const newPrimaryData: ContactInsertData = {
        email: newContactInfo.email,
        phoneNumber: newContactInfo.phoneNumber,
        linkedId: null,
        linkPrecedence: 'primary',
      };
      const { data: newPrimaryResult, error: insertError }: { data: Contact[] | null; error: PostgrestError | null } = await supabase
        .from('Contact')
        .insert([newPrimaryData])
        .select();
      
      if (insertError) throw insertError;
      
      if (newPrimaryResult && newPrimaryResult.length > 0) {
        primaryContact = newPrimaryResult[0];
        allContactsInvolved.push(primaryContact);
      }
    }
    
    // Prepare response
    const emailsSet = new Set<string>();
    const phoneNumbersSet = new Set<string>();
    const secondaryContactIds: number[] = [];

    // Re-fetch the final consolidated group to ensure data integrity for the response
    // This is important especially if primaryContact was just created or promoted.
    let finalConsolidatedContacts: Contact[] = [];
    if (primaryContact) {
        const { data: finalGroupData, error: finalGroupError }: {data: Contact[] | null, error: PostgrestError | null} = await supabase
            .from('Contact')
            .select('*')
            .or(`id.eq.${primaryContact.id},linkedId.eq.${primaryContact.id}`) // All linked to the true primary
            .order('createdAt', { ascending: true });
        if (finalGroupError) throw finalGroupError;
        finalConsolidatedContacts = finalGroupData || [];
    } else if (allContactsInvolved.length > 0) { // Fallback if primaryContact is somehow null but we have contacts
        finalConsolidatedContacts = allContactsInvolved;
    }


    finalConsolidatedContacts.forEach(c => {
      if (c.email) emailsSet.add(c.email);
      if (c.phoneNumber) phoneNumbersSet.add(c.phoneNumber);
      if (primaryContact && c.id !== primaryContact.id && c.linkPrecedence === 'secondary') {
        secondaryContactIds.push(c.id);
      }
    });
    
    secondaryContactIds.sort((a, b) => a - b);

    return res.status(200).json({
      contact: {
        primaryContactId: primaryContact?.id ?? null,
        emails: Array.from(emailsSet).filter(e => e !== null),
        phoneNumbers: Array.from(phoneNumbersSet).filter(p => p !== null),
        secondaryContactIds,
      },
    });

  } catch (err: any) {
    console.error('Error in /identify logic:', err.message, err.stack);
    return res.status(500).json({ error: err.message || 'Internal server error' });
  }
};