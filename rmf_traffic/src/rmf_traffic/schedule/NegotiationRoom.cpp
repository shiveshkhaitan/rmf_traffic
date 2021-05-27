/*
 * Copyright (C) 2021 Open Source Robotics Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
*/

#include <rmf_traffic/schedule/NegotiationRoom.hpp>

#include <iostream>
#include <map>
#include <utility>

namespace rmf_traffic {
namespace schedule {

//==============================================================================
std::string table_to_string(
  const std::vector<ParticipantId>& table)
{
  std::string output;
  for (const auto p : table)
    output += " " + std::to_string(p);
  return output;
}

//==============================================================================
template<typename T>
std::string ptr_to_string(T* ptr)
{
  std::stringstream str;
  str << ptr;
  return str.str();
}

class NegotiationRoom::Conversion
{
public:
  //==============================================================================
  static std::vector<Route> convert(const Itinerary& from)
  {
    std::vector<Route> output;
    for (const auto& r : from)
      output.emplace_back(*r);

    return output;
  }

  //==============================================================================
  static std::vector<Key> convert(const Negotiation::VersionedKeySequence& from)
  {
    std::vector<Key> output;
    output.reserve(from.size());
    for (const auto& key : from)
    {
      Key msg;
      msg.participant = key.participant;
      msg.version = key.version;
      output.push_back(msg);
    }

    return output;
  }

  //==============================================================================
  static Negotiation::VersionedKeySequence convert(const std::vector<Key>& from)
  {
    Negotiation::VersionedKeySequence output;
    output.reserve(from.size());
    for (const auto& key : from)
      output.push_back({key.participant, key.version});

    return output;
  }
};

//==============================================================================
NegotiationRoomInternal::NegotiationRoomInternal(Negotiation negotiation_)
: negotiation(std::move(negotiation_))
{
  // Do nothing
}

//==============================================================================
std::vector<Negotiation::TablePtr>
NegotiationRoomInternal::check_cache(const NegotiatorMap& negotiator_map)
{
  std::vector<Negotiation::TablePtr> new_tables;

  bool recheck = false;
  do
  {
    recheck = false;

    for (auto it = cached_proposals.begin(); it != cached_proposals.end(); )
    {
      const auto& proposal = *it;
      const auto search = negotiation.find(
        proposal.for_participant,
        NegotiationRoom::Conversion::convert(proposal.to_accommodate));

      if (search.deprecated())
      {
        cached_proposals.erase(it++);
        continue;
      }

      const auto table = search.table;
      if (table)
      {
        const bool updated = table->submit(
          proposal.itinerary, proposal.proposal_version);
        if (updated)
          new_tables.push_back(table);

        recheck = true;
        cached_proposals.erase(it++);
      }
      else
        ++it;
    }

    for (auto it = cached_rejections.begin(); it != cached_rejections.end(); )
    {
      const auto& rejection = *it;
      const auto search =
        negotiation.find(NegotiationRoom::Conversion::convert(rejection.
          table));

      if (search.deprecated())
      {
        cached_rejections.erase(it++);
        continue;
      }

      const auto table = search.table;
      if (table)
      {
        table->reject(
          rejection.table.back().version,
          rejection.rejected_by,
          rejection.alternatives);
        recheck = true;
        cached_rejections.erase(it++);
      }
      else
        ++it;
    }

    for (auto it = cached_forfeits.begin(); it != cached_forfeits.end(); )
    {
      const auto& forfeit = *it;
      const auto search =
        negotiation.find(NegotiationRoom::Conversion::convert(forfeit.table));

      if (search.deprecated())
      {
        cached_forfeits.erase(it++);
        continue;
      }

      const auto table = search.table;
      if (table)
      {
        table->forfeit(forfeit.table.back().version);
        recheck = true;
        cached_forfeits.erase(it++);
      }
      else
        ++it;
    }

  } while (recheck);

  auto remove_it = std::remove_if(new_tables.begin(), new_tables.end(),
      [](const Negotiation::TablePtr& table)
      {
        return table->forfeited() || table->defunct();
      });
  new_tables.erase(remove_it, new_tables.end());

  std::vector<Negotiation::TablePtr> respond_to;
  for (const auto& new_table : new_tables)
  {
    for (const auto& n : negotiator_map)
    {
      const auto participant = n.first;
      if (const auto r = new_table->respond(participant))
        respond_to.push_back(r);
    }
  }

  return respond_to;
}

//==============================================================================
class NegotiationRoom::Implementation
{
public:

  class Responder : public Negotiator::Responder
  {
  public:

    Responder(
      Implementation* const impl_,
      const Version version_,
      Negotiation::TablePtr table_)
    : impl(impl_),
      conflict_version(version_),
      table(std::move(table_)),
      table_version(table->version()),
      parent(table->parent()),
      parent_version(parent ? OptVersion(parent->version()) : OptVersion())
    {
      // Do nothing
    }

    template<typename... Args>
    static std::shared_ptr<Responder> make(
      Args&& ... args)
    {
      auto responder = std::make_shared<Responder>(std::forward<Args>(args)...);
      return responder;
    }

    void submit(
      std::vector<rmf_traffic::Route> itinerary,
      std::function<UpdateVersion()> approval_callback) const final
    {
      responded = true;
      if (table->defunct())
        return;

      if (table->submit(itinerary, table_version+1))
      {
        impl->approvals[conflict_version][table] = {
          table->sequence(),
          std::move(approval_callback)
        };

        impl->publish_proposal(conflict_version, *table);

        if (impl->worker)
        {
          for (const auto& c : table->children())
          {
            const auto n_it = impl->negotiators->find(c->participant());
            if (n_it == impl->negotiators->end())
              continue;

            impl->worker->schedule(
              [viewer = c->viewer(),
              negotiator = n_it->second.get(),
              responder = make(impl, conflict_version, c)]()
              {
                negotiator->respond(viewer, responder);
              });
          }
        }
      }
    }

    void reject(const Alternatives& alternatives) const final
    {
      responded = true;
      if (parent && !parent->defunct())
      {
        // We will reject the parent to communicate that its proposal is not
        // feasible for us.
        if (parent->reject(*parent_version, table->participant(), alternatives))
        {
          impl->publish_rejection(
            conflict_version, *parent, table->participant(), alternatives);

          // TODO(MXG): We don't schedule a response to the rejection for
          // async negotiations, because the ROS2 subscription will do that for
          // us whether we want it to or not.
//          if (impl->worker)
//          {
//            const auto n_it = impl->negotiators->find(parent->participant());
//            if (n_it == impl->negotiators->end())
//              return;

//            impl->worker->schedule(
//                  [viewer = parent->viewer(),
//                   negotiator = n_it->second.get(),
//                   responder = make(impl, conflict_version, parent)]()
//            {
//              negotiator->respond(viewer, responder);
//            });
//          }
        }
      }
    }

    void forfeit(const std::vector<ParticipantId>& /*blockers*/) const final
    {
      responded = true;
      if (!table->defunct())
      {
        // TODO(MXG): Consider using blockers to invite more participants into the
        // negotiation
        table->forfeit(table_version);
        impl->publish_forfeit(conflict_version, *table);
      }
    }

    void timeout() const
    {
      if (!responded)
        forfeit({});
    }

    ~Responder() override
    {
      timeout();
    }

  private:

    Implementation* const impl;
    const Version conflict_version;

    const Negotiation::TablePtr table;
    Version table_version;

    using OptVersion = rmf_utils::optional<Version>;
    const Negotiation::TablePtr parent;
    OptVersion parent_version;

    mutable bool responded = false;
  };

  std::shared_ptr<const Snappable> viewer;
  PublisherCallbacks publisher_callbacks;
  std::shared_ptr<Worker> worker;
  rmf_traffic::Duration timeout = std::chrono::seconds(15);

  using NegotiatorPtr = std::unique_ptr<Negotiator>;
  using NegotiatorMap = std::unordered_map<ParticipantId, NegotiatorPtr>;
  using NegotiationMapPtr = std::shared_ptr<NegotiatorMap>;
  using WeakNegotiationMapPtr = std::weak_ptr<NegotiatorMap>;
  NegotiationMapPtr negotiators;

  struct Entry
  {
    bool participating;
    NegotiationRoomInternal room;
  };

  using NegotiationMap = std::unordered_map<Version, Entry>;

  // The negotiations that this Negotiation class is involved in
  NegotiationMap negotiations;

  using TablePtr = Negotiation::TablePtr;
  using UpdateVersion = rmf_utils::optional<ItineraryVersion>;
  struct CallbackEntry
  {
    Negotiation::VersionedKeySequence sequence;
    std::function<UpdateVersion()> callback;
  };

  using ApprovalCallbackMap = std::unordered_map<TablePtr, CallbackEntry>;
  using Approvals = std::unordered_map<Version, ApprovalCallbackMap>;
  Approvals approvals;

  // Status update publisher_callbacks
  using TableViewPtr = Negotiation::Table::ViewerPtr;
  using StatusUpdateCallback =
    std::function<void (uint64_t conflict_version, TableViewPtr& view)>;
  StatusUpdateCallback status_callback;

  using StatusConclusionCallback =
    std::function<void (uint64_t conflict_version, bool success)>;
  StatusConclusionCallback conclusion_callback;

  uint retained_history_count = 0;
  std::map<Version, Negotiation> history;

  Implementation(
    std::shared_ptr<const Snappable> viewer_,
    PublisherCallbacks publisher_callbacks,
    std::shared_ptr<SubscriptionCallbacks> subscription_callbacks,
    std::shared_ptr<Worker> worker_)
  : viewer(std::move(viewer_)),
    publisher_callbacks(std::move(publisher_callbacks)),
    worker(std::move(worker_)),
    negotiators(std::make_shared<NegotiatorMap>())
  {
    subscription_callbacks->repeat = std::bind(
      &Implementation::receive_repeat_request, this, std::placeholders::_1);
    subscription_callbacks->notice = std::bind(&Implementation::receive_notice,
        this, std::placeholders::_1);
    subscription_callbacks->proposal = std::bind(
      &Implementation::receive_proposal, this, std::placeholders::_1);
    subscription_callbacks->rejection = std::bind(
      &Implementation::receive_rejection, this, std::placeholders::_1);
    subscription_callbacks->forfeit = std::bind(
      &Implementation::receive_forfeit, this, std::placeholders::_1);
    subscription_callbacks->conclusion = std::bind(
      &Implementation::receive_conclusion, this, std::placeholders::_1);
  }

  void receive_repeat_request(const Repeat& msg)
  {
    // Ignore if it's asking for a repeat of the conflict notice
    if (msg.table.empty())
      return;

    // Ignore if we haven't heard of this negotiation
    const auto negotiate_it = negotiations.find(msg.conflict_version);
    if (negotiate_it == negotiations.end())
      return;

    // Ignore if we aren't participating in this negotiation
    if (!negotiate_it->second.participating)
      return;

    // Ignore if we aren't managing the relevant negotiator
    const ParticipantId for_participant = msg.table.back();
    const auto negotiator_it = negotiators->find(for_participant);
    if (negotiator_it == negotiators->end())
      return;

    auto to_accommodate = msg.table;
    to_accommodate.pop_back();

    const auto table = negotiate_it->second.room.negotiation.table(
      for_participant, to_accommodate);
    if (!table)
    {
      std::string error =
        "[rmf_traffic_ros2::schedule::Negotiation] A repeat was requested "
        "for a table that does not exist. Negotiation ["
        + std::to_string(msg.conflict_version) + "], table ["
        + table_to_string(msg.table) + " ]";

      std::cout << error << std::endl;

      return;
    }

    publish_proposal(msg.conflict_version, *table);
  }

  void respond_to_queue(
    std::vector<TablePtr> queue,
    Version conflict_version)
  {
    while (!queue.empty())
    {
      const auto top = queue.back();
      queue.pop_back();

      if (top->defunct())
        continue;

      if (!top->submission())
      {
        const auto n_it = negotiators->find(top->participant());
        if (n_it == negotiators->end())
          continue;

        // TODO(MXG): Make this limit configurable
        if (top->version() > 3)
        {
          // Give up on this table at this point to avoid an infinite loop
          top->forfeit(top->version());
          publish_forfeit(conflict_version, *top);
          continue;
        }

        const auto& negotiator = n_it->second;
        negotiator->respond(
          top->viewer(), Responder::make(this, conflict_version, top));
      }

      if (top->submission())
      {
        for (const auto& c : top->children())
          queue.push_back(c);
      }
      else if (const auto& parent = top->parent())
      {
        if (parent->rejected())
          queue.push_back(parent);
      }
    }
  }

  void receive_notice(const Notice& msg)
  {
    bool relevant = false;
    for (const auto p : msg.participants)
    {
      if (negotiators->find(p) != negotiators->end())
      {
        relevant = true;
        break;
      }
    }

    auto new_negotiation = Negotiation::make(
      viewer->snapshot(), msg.participants);
    if (!new_negotiation)
    {
      // TODO(MXG): This is a temporary hack to deal with situations where a
      // conflict occurs before this node is aware of other relevant schedule
      // participants. We will refuse the negotiation and then expect it to be
      // retriggered some time later. Hopefully this node will know about the
      // schedule participant by then.
      //
      // A better system will be to use ReactiveX to observe the schedule and
      // try to open the negotiation again once the participant information is
      // available.
      Refusal refusal;
      refusal.conflict_version = msg.conflict_version;
      if (publisher_callbacks.refusal)
      {
        publisher_callbacks.refusal(refusal);
      }

      const auto n_it = negotiations.find(msg.conflict_version);
      if (n_it != negotiations.end())
        negotiations.erase(n_it);
      return;
    }

    const auto insertion = negotiations.insert(
      {msg.conflict_version, Entry{relevant, *std::move(new_negotiation)}});

    const bool is_new = insertion.second;
    bool& participating = insertion.first->second.participating;
    auto& room = insertion.first->second.room;
    Negotiation& negotiation = room.negotiation;

    if (!is_new)
    {
      const auto& old_participants = negotiation.participants();
      for (const auto p : msg.participants)
      {
        if (old_participants.count(p) == 0)
        {
          negotiation.add_participant(p);
        }
      }
    }

    if (!relevant)
    {
      // No response needed
      return;
    }

    // TODO(MXG): Is the participating flag even relevant?
    participating = true;

    std::vector<TablePtr> queue;
    for (const auto p : negotiation.participants())
      queue.push_back(negotiation.table(p, {}));

    respond_to_queue(queue, msg.conflict_version);
  }

  void receive_proposal(const Proposal& msg)
  {
    const auto negotiate_it = negotiations.find(msg.conflict_version);
    if (negotiate_it == negotiations.end())
    {
      // This negotiation has probably been completed already
      return;
    }

    const bool participating = negotiate_it->second.participating;
    auto& room = negotiate_it->second.room;
    Negotiation& negotiation = room.negotiation;
    const auto search =
      negotiation.find(
      msg.for_participant, Conversion::convert(msg.to_accommodate));

    if (search.deprecated())
      return;

    const auto received_table = search.table;
    if (!received_table)
    {
      std::string error =
        "[rmf_traffic_ros2::schedule::Negotiation::receive_proposal] "
        "Receieved a proposal for negotiation ["
        + std::to_string(msg.conflict_version) + "] that builds on an "
        "unknown table: [";
      for (const auto p : msg.to_accommodate)
      {
        error += " " + std::to_string(p.participant)
          + ":" + std::to_string(p.version);
      }
      error += " " + std::to_string(msg.for_participant) + " ]";

      std::cout << error << std::endl;
      room.cached_proposals.push_back(msg);
      return;
    }

    // We'll keep track of these negotiations whether or not we're participating
    // in them, because one of our negotiators might get added to it in the
    // future
    const bool updated = received_table->submit(
      msg.itinerary, msg.proposal_version);

    if (!updated)
      return;

    if (status_callback)
    {
      auto table_view = received_table->viewer();
      status_callback(msg.conflict_version, table_view);
    }

    std::vector<TablePtr> queue = room.check_cache(*negotiators);

    if (!participating)
      return;

    for (const auto& n : *negotiators)
    {
      if (const auto respond_to = received_table->respond(n.first))
        queue.push_back(respond_to);
    }

    respond_to_queue(queue, msg.conflict_version);
  }

  void receive_rejection(const Rejection& msg)
  {
    const auto negotiate_it = negotiations.find(msg.conflict_version);
    if (negotiate_it == negotiations.end())
    {
      // We don't need to worry about caching an unknown rejection, because it
      // is impossible for a proposal that was produced by this negotiation
      // instance to be rejected without us being aware of that proposal.
      return;
    }

    auto& room = negotiate_it->second.room;
    Negotiation& negotiation = room.negotiation;
    const auto search = negotiation.find(Conversion::convert(msg.table));

    if (search.deprecated())
      return;

    const auto table = search.table;
    if (!table)
    {
      std::string error =
        "[rmf_traffic_ros2::schedule::Negotiation::receive_rejection] "
        "Receieved a rejection for negotiation ["
        + std::to_string(msg.conflict_version) + "] for an "
        "unknown table: [";
      for (const auto p : msg.table)
      {
        error += " " + std::to_string(p.participant)
          + ":" + std::to_string(p.version);
      }
      error += " ]";

      std::cout << error << std::endl;

      room.cached_rejections.push_back(msg);
      return;
    }

    const bool updated = table->reject(
      msg.table.back().version, msg.rejected_by, msg.alternatives);

    if (!updated)
      return;

    if (status_callback)
    {
      auto table_view = table->viewer();
      status_callback(msg.conflict_version, table_view);
    }

    std::vector<TablePtr> queue = room.check_cache(*negotiators);

    if (!negotiate_it->second.participating)
      return;

    queue.push_back(table);
    respond_to_queue(queue, msg.conflict_version);
  }

  void receive_forfeit(const Forfeit& msg)
  {
    const auto negotiate_it = negotiations.find(msg.conflict_version);
    if (negotiate_it == negotiations.end())
    {
      // We don't need to worry about caching an unknown rejection, because it
      // is impossible for a proposal that was produced by this negotiation
      // instance to be rejected without us being aware of that proposal.
      return;
    }

    auto& room = negotiate_it->second.room;
    Negotiation& negotiation = room.negotiation;
    const auto search = negotiation.find(Conversion::convert(msg.table));
    if (search.deprecated())
      return;

    const auto table = search.table;
    if (!table)
    {
      room.cached_forfeits.push_back(msg);
      return;
    }

    table->forfeit(msg.table.back().version);

    if (status_callback)
    {
      auto table_view = table->viewer();
      status_callback(msg.conflict_version, table_view);
    }

    respond_to_queue(room.check_cache(*negotiators), msg.conflict_version);
  }

  void dump_conclusion_info(
    const Conclusion& msg,
    const Approvals::const_iterator& approval_callback_it,
    const Negotiation& negotiation)
  {
    const auto full_sequence = Conversion::convert(msg.table);

    std::string err =
      "\n !!!!!!!!!!!! Impossible situation encountered for Negotiation ["
      + std::to_string(msg.conflict_version)
      + "] No approval publisher_callbacks found?? Sequence: [";
    for (const auto s : msg.table)
    {
      err += " " + std::to_string(s.participant)
        + ":" + std::to_string(s.version);
    }
    err += " ] ";

    if (msg.resolved)
    {
      err += "Tables with acknowledgments for this negotiation:";

      const auto& approval_callbacks = approval_callback_it->second;
      for (const auto& cb : approval_callbacks)
      {
        const auto table = cb.first;
        err += "\n -- " + ptr_to_string(table.get()) + " |";

        for (const auto& s : table->sequence())
        {
          err += " " + std::to_string(s.participant) + ":"
            + std::to_string(s.version);
        }
      }

      err += "\nCurrent relevant tables in the negotiation:";
      for (std::size_t i = 1; i <= msg.table.size(); ++i)
      {
        std::vector<ParticipantId> sequence;
        for (std::size_t j = 0; j < i; ++j)
          sequence.push_back(full_sequence[j].participant);

        const auto table = negotiation.table(sequence);
        err += "\n -- " + ptr_to_string(table.get()) + " |";

        for (std::size_t j = 0; j < i; ++j)
        {
          err += " " + std::to_string(msg.table[j].participant) + ":"
            + std::to_string(msg.table[j].version);
        }
      }
    }
    else
    {
      err += "Negotiation participants for this node:";
      for (const auto p : negotiation.participants())
      {
        if (negotiators->count(p) != 0)
          err += " " + std::to_string(p);
      }
    }

    print_negotiation_status(msg.conflict_version, negotiation);
  }

  void receive_conclusion(const Conclusion& msg)
  {
    const auto negotiate_it = negotiations.find(msg.conflict_version);
    if (negotiate_it == negotiations.end())
    {
      // We don't need to worry about concluding unknown negotiations
      return;
    }

    const bool participating = negotiate_it->second.participating;
    auto& room = negotiate_it->second.room;
    Negotiation& negotiation = room.negotiation;
    const auto full_sequence = Conversion::convert(msg.table);

    if (participating)
    {
      std::vector<ParticipantAck> acknowledgments;

      const auto approval_callback_it = approvals.find(msg.conflict_version);
      if (msg.resolved)
      {
        assert(approval_callback_it != approvals.end());
        const auto& approval_callbacks = approval_callback_it->second;

        for (std::size_t i = 1; i <= msg.table.size(); ++i)
        {
          const auto sequence = Negotiation::VersionedKeySequence(
            full_sequence.begin(), full_sequence.begin()+i);
          const auto participant = sequence.back().participant;

          const auto search = negotiation.find(sequence);
          if (search.absent())
          {
            // If the Negotiation never knew about this sequence, then we cannot
            // possibly have any approval publisher_callbacks waiting for it. This may
            // happen towards the end of a Negotiation sequence if the remaining
            // tables belong to a different node.
            break;
          }

          auto approve_it = approval_callbacks.end();
          if (search)
          {
            approve_it = approval_callbacks.find(search.table);
          }
          else
          {
            assert(search.deprecated());
            // The final table was somehow deprecated. In principle this
            // shouldn't happen if all Negotiation participants are "consistent"
            // about what they propose. However, due to race conditions and
            // implementation details, we cannot guarantee consistency.
            // Therefore this situation is possible, and we should just try our
            // best to cope with it gracefully.

            if (negotiators->find(participant) == negotiators->end())
            {
              // We do not have a negotiator for this participant, so there is
              // no point searching for an approval callback for it.
              continue;
            }

            // We will do a brute-force search through our approval publisher_callbacks
            // to dig up the relevant one.
            for (auto a_it = approval_callbacks.begin();
              a_it != approval_callbacks.end();
              ++a_it)
            {
              if (a_it->second.sequence == sequence)
              {
                approve_it = a_it;
                break;
              }
            }
          }

          if (approve_it != approval_callbacks.end())
          {
            const auto& entry = approve_it->second;
            ParticipantAck p_ack;
            p_ack.participant = entry.sequence.back().participant;
            p_ack.updating = false;
            const auto& approval_cb = entry.callback;
            if (approval_cb)
            {
              const auto update_version = approval_cb();
              if (update_version)
              {
                p_ack.updating = true;
                p_ack.itinerary_version = *update_version;
              }
            }

            acknowledgments.emplace_back(std::move(p_ack));
          }
          else
          {
            // If we couldn't find an approval callback for this table of the
            // conclusion, then that should not be a negotiator for the table.
            if (negotiators->find(participant) != negotiators->end())
            {
              dump_conclusion_info(msg, approval_callback_it, negotiation);
              assert(negotiators->find(participant) == negotiators->end());
            }
          }
        }
      }
      else
      {
        ParticipantAck p_ack;
        p_ack.updating = false;
        for (const auto p : negotiation.participants())
        {
          if (negotiators->count(p) != 0)
          {
            p_ack.participant = p;
            acknowledgments.push_back(p_ack);
          }
        }
      }

      // Acknowledge that we know about this conclusion
      Ack ack;
      ack.conflict_version = msg.conflict_version;
      ack.acknowledgments = std::move(acknowledgments);

      if (ack.acknowledgments.empty())
      {
        // If we are participating in this negotiation, then the
        // acknowledgments must not be empty, or else there is a bug somewhere.
        dump_conclusion_info(msg, approval_callback_it, negotiation);
        assert(!ack.acknowledgments.empty());
      }

      if (publisher_callbacks.ack)
      {
        publisher_callbacks.ack(ack);
      }
      // TODO(MXG): Should we consider a more robust cache cleanup strategy?

      if (approval_callback_it != approvals.end())
        approvals.erase(approval_callback_it);
    }

    if (conclusion_callback)
      conclusion_callback(msg.conflict_version, msg.resolved);

    // add to retained history
    if (retained_history_count > 0)
    {
      while (history.size() >= retained_history_count)
        history.erase(history.begin());

      history.emplace(
        msg.conflict_version, std::move(negotiate_it->second.room.negotiation));
    }

    // Erase these entries because the negotiation has concluded
    negotiations.erase(negotiate_it);
  }

  void publish_proposal(
    const Version conflict_version,
    const Negotiation::Table& table) const
  {
    Proposal msg;
    msg.conflict_version = conflict_version;
    msg.proposal_version = table.version();

    assert(table.submission());
    msg.itinerary = Conversion::convert(*table.submission());
    msg.for_participant = table.participant();
    msg.to_accommodate = Conversion::convert(table.sequence());

    // Make sure to pop the back off of msg.to_accommodate, because we don't
    // want to include this table's final sequence key. That final key is
    // provided by for_participant.
    msg.to_accommodate.pop_back();

    if (publisher_callbacks.proposal)
    {
      publisher_callbacks.proposal(msg);
    }
  }

  void publish_rejection(
    const Version conflict_version,
    const Negotiation::Table& table,
    const ParticipantId rejected_by,
    const Negotiation::Alternatives& alternatives) const
  {
    Rejection msg;
    msg.conflict_version = conflict_version;
    msg.table = Conversion::convert(table.sequence());
    msg.rejected_by = rejected_by;
    msg.alternatives = alternatives;

    if (publisher_callbacks.rejection)
    {
      publisher_callbacks.rejection(msg);
    }
  }

  void publish_forfeit(
    const Version conflict_version,
    const Negotiation::Table& table)
  {
    Forfeit msg;
    msg.conflict_version = conflict_version;
    msg.table = Conversion::convert(table.sequence());

    if (publisher_callbacks.forfeit)
    {
      publisher_callbacks.forfeit(msg);
    }
  }

  struct Handle
  {
    Handle(
      const ParticipantId for_participant_,
      const NegotiationMapPtr& map)
    : for_participant(for_participant_),
      weak_map(map)
    {
      // Do nothing
    }

    ParticipantId for_participant;
    WeakNegotiationMapPtr weak_map;

    ~Handle()
    {
      if (const auto map = weak_map.lock())
        map->erase(for_participant);
    }
  };

  std::shared_ptr<void> register_negotiator(
    const ParticipantId for_participant,
    NegotiatorPtr negotiator)
  {
    const auto insertion = negotiators->insert(
      std::make_pair(for_participant, std::move(negotiator)));

    if (!insertion.second)
    {
      // *INDENT-OFF*
      throw std::runtime_error(
        "[rmf_traffic_ros2::schedule::Negotiaton] Attempt to register a "
        "duplicate negotiator for participant ["
        + std::to_string(for_participant) + "]");
      // *INDENT-ON*
    }

    return std::make_shared<Handle>(for_participant, negotiators);
  }

  void set_retained_history_count(uint count)
  {
    retained_history_count = count;
  }

  TableViewPtr table_view(
    uint64_t conflict_version,
    const std::vector<ParticipantId>& sequence) const
  {
    const auto negotiate_it = negotiations.find(conflict_version);
    Negotiation::ConstTablePtr table;

    if (negotiate_it == negotiations.end())
    {
      const auto history_it = history.find(conflict_version);
      if (history_it == history.end())
      {
        std::cout << "Conflict version " << conflict_version <<
          " does not exist. It may have been successful and wiped" << std::endl;
        return nullptr;
      }

      table = history_it->second.table(sequence);
    }
    else
    {
      const auto& room = negotiate_it->second.room;
      const Negotiation& negotiation = room.negotiation;
      table = negotiation.table(sequence);
    }

    if (!table)
    {
      std::cout << "Table not found" << std::endl;
      return nullptr;
    }

    return table->viewer();
  }

  void on_status_update(StatusUpdateCallback cb)
  {
    status_callback = std::move(cb);
  }

  void on_conclusion(StatusConclusionCallback cb)
  {
    conclusion_callback = std::move(cb);
  }
};

//==============================================================================
NegotiationRoom::NegotiationRoom(
  std::shared_ptr<const Snappable> viewer,
  PublisherCallbacks publisher_callbacks,
  std::shared_ptr<SubscriptionCallbacks> subscription_callbacks,
  std::shared_ptr<Worker> worker)
: _pimpl(rmf_utils::make_unique_impl<Implementation>(
      std::move(viewer),
      std::move(publisher_callbacks),
      std::move(subscription_callbacks),
      std::move(worker)))
{
  // Do nothing
}

//==============================================================================
void NegotiationRoom::on_status_update(StatusUpdateCallback cb)
{
  _pimpl->on_status_update(std::move(cb));
}

//==============================================================================
void NegotiationRoom::on_conclusion(StatusConclusionCallback cb)
{
  _pimpl->on_conclusion(std::move(cb));
}

//==============================================================================
NegotiationRoom&
NegotiationRoom::timeout_duration(rmf_traffic::Duration duration)
{
  _pimpl->timeout = duration;
  return *this;
}

//==============================================================================
rmf_traffic::Duration NegotiationRoom::timeout_duration() const
{
  return _pimpl->timeout;
}

//==============================================================================
NegotiationRoom::TableViewPtr NegotiationRoom::table_view(
  uint64_t conflict_version,
  const std::vector<ParticipantId>& sequence) const
{
  return _pimpl->table_view(conflict_version, sequence);
}

//==============================================================================
void NegotiationRoom::set_retained_history_count(uint count)
{
  return _pimpl->set_retained_history_count(count);
}

//==============================================================================
std::shared_ptr<void> NegotiationRoom::register_negotiator(
  ParticipantId for_participant,
  std::unique_ptr<Negotiator> negotiator)
{
  return _pimpl->register_negotiator(for_participant, std::move(negotiator));
}

//==============================================================================
void print_negotiation_status(
  Version conflict_version,
  const Negotiation& negotiation)
{
  std::cout << "\n[" << conflict_version << "] Active negotiation:";
  for (const auto p : negotiation.participants())
    std::cout << " " << p;
  std::cout << std::endl;

  std::vector<Negotiation::ConstTablePtr> terminal;
  std::vector<Negotiation::ConstTablePtr> queue;
  for (const auto p : negotiation.participants())
  {
    const auto p_table = negotiation.table(p, {});
    assert(p_table);
    queue.push_back(p_table);
  }

  while (!queue.empty())
  {
    Negotiation::ConstTablePtr top = queue.back();
    queue.pop_back();

    std::vector<Negotiation::ConstTablePtr> children = top->children();
    if (children.empty())
    {
      terminal.push_back(top);
    }
    else
    {
      for (const auto& child : children)
      {
        assert(child);
        queue.push_back(child);
      }
    }
  }

  std::cout << "    Current negotiation state";
  for (const auto& t : terminal)
  {
    std::cout << "\n --";
    const bool finished = static_cast<bool>(t->submission());
    const bool rejected = t->rejected();
    const bool forfeited = t->forfeited();
    const auto sequence = t->sequence();
    for (std::size_t i = 0; i < sequence.size(); ++i)
    {
      const auto& s = sequence[i];
      if (i == t->sequence().size()-1)
      {
        if (forfeited)
          std::cout << " <" << s.participant << ":" << s.version << ">";
        else if (rejected)
          std::cout << " {" << s.participant << ":" << s.version << "}";
        else if (!finished)
          std::cout << " [" << s.participant << ":" << s.version << "]";
        else
          std::cout << " >" << s.participant << ":" << s.version << "<";
      }
      else
      {
        std::cout << " " << s.participant << ":" << s.version;
      }
    }
  }
  std::cout << "\n" << std::endl;
}

} // namespace schedule
} // namespace rmf_traffic
